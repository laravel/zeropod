package reuse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/cilium/ebpf"
	"github.com/laravel/zeropod/activator"
	"golang.org/x/sys/unix"
)

type listenerGroup struct {
	wake      wakeListener
	probe     probeListener
	app       appListener
	forwarder *forwarder
	reuse     *reuseportObjects
}

type wakeListener struct {
	ln   *net.TCPListener
	file *os.File
}

type appListener struct {
	fd  int
	uid uint64
}

type listenerKey struct {
	port    uint16
	network activator.Network
}

func (wl *wakeListener) closeListener() {
	if wl.ln != nil {
		_ = wl.ln.Close()
		wl.ln = nil
	}
	if wl.file != nil {
		_ = wl.file.Close()
		wl.file = nil
	}
}

func (wl *wakeListener) close() {
	wl.closeListener()
}

// listenWake will call listenReuseport and store the newly created listener in
// wake. Needs to be called inside the target network namespace.
func (act *Activator) listenWake(port uint16, network activator.Network, lg *listenerGroup) error {
	act.log.Debugf("listening wake: %d %s", port, network)
	ln, err := listenReuseport(port, network, int(lg.app.uid))
	if err != nil {
		return fmt.Errorf("wake listener: %w", err)
	}
	lg.wake.ln = ln
	return nil
}

func (act *Activator) attachWake(network activator.Network, lg *listenerGroup) error {
	var dupFd int
	var dupErr error
	if err := act.attachNetListener(lg.wake.ln, wakeKey, lg.reuse.Listeners, lg.reuse.SelectOrMigrate, func(fd uintptr) {
		dupFd, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0)
	}); err != nil {
		return err
	}
	if dupErr != nil {
		return dupErr
	}

	var stat syscall.Stat_t
	if err := syscall.Fstat(dupFd, &stat); err != nil {
		_ = unix.Close(dupFd)
		return err
	}
	act.wakeInodes = append(act.wakeInodes, stat.Ino)

	lg.wake.file = os.NewFile(uintptr(dupFd), "wake")
	go act.watchWake(lg.wake.file, network)

	return nil
}

// watchWake polls the wake listener without ever accepting and calls wake as
// soon as the poll returns something. It returns without waking once the
// listener is closed.
func (act *Activator) watchWake(file *os.File, network activator.Network) {
	rc, err := file.SyscallConn()
	if err != nil {
		act.log.WithError(err).Error("getting wake listener conn")
		return
	}
	var pollErr error
	if err := rc.Read(func(fd uintptr) bool {
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		for {
			n, err := unix.Poll(fds, 0)
			if err == unix.EINTR {
				continue
			}
			pollErr = err
			return err != nil || n > 0
		}
	}); err != nil {
		if _, statErr := file.Stat(); !errors.Is(statErr, os.ErrClosed) {
			act.log.WithError(err).Error("waiting for wake listener")
		}
		return
	}
	if pollErr != nil {
		act.log.WithError(pollErr).Error("polling wake listener")
		return
	}
	act.log.Info("socket activity detected, waking up")
	if err := act.wake(network); err != nil {
		act.log.WithError(err).Error("wake")
	}
}

func (act *Activator) registerListeners(pid int) error {
	act.mu.Lock()
	defer act.mu.Unlock()

	before := time.Now()
	listeners, err := act.listenerFds(pid)
	if err != nil {
		return err
	}
	if !act.containsAllPorts(listeners) {
		for _, ln := range listeners {
			if ln.FD != nil {
				_ = ln.FD.Close()
			}
		}
		return fmt.Errorf("%w: not all ports %v in listeners: %s", activator.ErrExpectedListeningSockets, act.ports, listeners)
	}
	act.log.Debugf("getting listeners in %s", time.Since(before))

	for _, l := range listeners {
		if !slices.Contains(act.ports, l.Port) {
			continue
		}
		if l.FD == nil {
			continue
		}
		//nolint:errcheck
		defer l.FD.Close()

		key := listenerKey{port: l.Port, network: l.Network}
		if _, ok := act.listeners[key]; !ok {
			objs := &reuseportObjects{}
			if err := loadReuseportObjects(objs, &ebpf.CollectionOptions{}); err != nil {
				return fmt.Errorf("loading reuseport objects: %w", err)
			}
			if act.probeAddr != nil {
				if err := objs.ProbeAddr.Set(act.probeAddrValue()); err != nil {
					return err
				}
			}
			act.listeners[key] = &listenerGroup{
				reuse: objs,
			}
		}
		ln := act.listeners[key]
		act.log.Debugf("registering ln %d port %d net %s ino %d", l.FD.Fd(), l.Port, l.Network, l.Inode)
		if err := act.attachListener(appKey, l.FD.Fd(), ln.reuse.Listeners, ln.reuse.SelectOrMigrate); err != nil {
			return fmt.Errorf("registering listener: %w", err)
		}
		act.log.Debugf("caching port %d fd %d uid %d", l.Port, l.OrigFd, l.UID)
		act.listeners[key].app = appListener{fd: l.OrigFd, uid: l.UID}
	}
	if len(listeners) == 0 {
		return fmt.Errorf("%w: no sockets found", activator.ErrExpectedListeningSockets)
	}
	return nil
}

func (act *Activator) containsAllPorts(listeners activator.Listeners) bool {
	for _, port := range act.ports {
		if !slices.ContainsFunc(listeners, func(ln activator.Listener) bool {
			return ln.Port == port && ln.FD != nil
		}) {
			return false
		}
	}
	return true
}

func (act *Activator) probeAddrValue() [16]byte {
	if act.probeAddr == nil {
		return [16]byte{}
	}
	var ebpfProbeAddr [16]byte
	if act.probeAddr.Is4() {
		a4 := act.probeAddr.As4()
		copy(ebpfProbeAddr[:], a4[:])
	} else {
		a16 := act.probeAddr.As16()
		copy(ebpfProbeAddr[:], a16[:])
	}
	return ebpfProbeAddr
}

func (act *Activator) listenerFds(pid int) (activator.Listeners, error) {
	l, err := act.listenerFdsFromCache(pid)
	if err == nil && len(l) > 0 && len(l) == len(act.listeners) {
		return l, nil
	}
	// close fds in case the cache returned partial listeners
	for _, ln := range l {
		if ln.FD != nil {
			_ = ln.FD.Close()
		}
	}
	listeners, err := activator.GetListenersOfPIDWithFD(act.log.Context, pid, act.wakeInodes...)
	if err != nil {
		return nil, err
	}

	return listeners, nil
}

// attachListener attaches select_or_migrate to the listeners reuseport group
// and puts it into the key slot.
func (act *Activator) attachListener(key uint32, lnFd uintptr, bpfMap *ebpf.Map, prog *ebpf.Program) error {
	if err := unix.SetsockoptInt(int(lnFd), unix.SOL_SOCKET,
		unix.SO_ATTACH_REUSEPORT_EBPF, prog.FD()); err != nil {
		return fmt.Errorf("attach reuseport prog: %w", err)
	}
	if err := bpfMap.Update(&key, uint64(lnFd), ebpf.UpdateAny); err != nil {
		return fmt.Errorf("sockarray app: %w", err)
	}
	return nil
}

// attachNetListener gets the fd of the [net.Listener] and then attaches it to
// the reuseport group into the key slot.
func (act *Activator) attachNetListener(ln net.Listener, key uint32, bpfMap *ebpf.Map, prog *ebpf.Program, fdfunc func(fd uintptr)) error {
	sc, err := ln.(syscall.Conn).SyscallConn()
	if err != nil {
		_ = ln.Close()
		return err
	}
	var registerErr error
	if err := sc.Control(func(fd uintptr) {
		registerErr = act.attachListener(key, fd, bpfMap, prog)
		if registerErr == nil {
			if fdfunc != nil {
				fdfunc(fd)
			}
		}
	}); err != nil {
		_ = ln.Close()
		return err
	}
	if registerErr != nil {
		_ = ln.Close()
		return registerErr
	}
	return nil
}

// listenReuseport opens a TCP listener with SO_REUSEPORT
func listenReuseport(port uint16, network activator.Network, uid int) (*net.TCPListener, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			if err := c.Control(func(fd uintptr) {
				serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
				if serr != nil {
					return
				}
				if serr = unix.Fchown(int(fd), uid, -1); serr != nil {
					return
				}
			}); err != nil {
				return err
			}
			return serr
		},
	}
	lc.SetMultipathTCP(false)
	ln, err := lc.Listen(context.Background(), string(network), fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	return ln.(*net.TCPListener), nil
}

func (act *Activator) listenerFdsFromCache(pid int) (activator.Listeners, error) {
	cache := act.listeners
	if len(cache) == 0 {
		return nil, nil
	}

	listeners := activator.Listeners{}
	pids, err := activator.ContainerPids(pid)
	if err != nil {
		return nil, err
	}
	resolved := map[listenerKey]struct{}{}
	for _, cpid := range pids {
		// bail out early as we found all listeners
		if len(resolved) == len(cache) {
			break
		}

		pidfd, err := unix.PidfdOpen(cpid, 0)
		if err != nil {
			continue
		}
		//nolint:errcheck
		defer unix.Close(pidfd)

		for k, v := range cache {
			if _, ok := resolved[k]; ok {
				continue
			}
			fd, err := unix.PidfdGetfd(pidfd, v.app.fd, 0)
			if err != nil {
				continue
			}
			var stat unix.Stat_t
			if err := unix.Fstat(int(fd), &stat); err != nil {
				_ = unix.Close(fd)
				continue
			}

			sockaddr, err := unix.Getsockname(fd)
			if err != nil {
				_ = unix.Close(fd)
				continue
			}

			var port int
			switch sa := sockaddr.(type) {
			case *unix.SockaddrInet4:
				port = sa.Port
			case *unix.SockaddrInet6:
				port = sa.Port
			}
			if k.port != uint16(port) {
				_ = unix.Close(fd)
				continue
			}
			network, err := activator.GetNetworkFromSock(fd)
			if err != nil {
				_ = unix.Close(fd)
				continue
			}
			if network != k.network {
				_ = unix.Close(fd)
				continue
			}
			resolved[k] = struct{}{}
			listeners = append(listeners, activator.Listener{
				Port:    k.port,
				Network: network,
				FD:      os.NewFile(uintptr(fd), ""),
				OrigFd:  v.app.fd,
				UID:     v.app.uid,
				Inode:   uint32(stat.Ino),
			})
		}
	}
	return listeners, nil
}
