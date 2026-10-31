package shim

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/textproto"
	"strings"

	"github.com/containerd/log"
	"github.com/laravel/zeropod/activator"
)

func (c *Container) detectProbe(ctx context.Context) activator.ConnHook {
	if c.cfg.DisableProbeDetection {
		return func(conn net.Conn) (net.Conn, bool, error) {
			return conn, true, nil
		}
	}
	return func(netConn net.Conn) (net.Conn, bool, error) {
		if !isKubeletAddr(ctx, netConn.RemoteAddr(), c.activator) {
			return netConn, true, nil
		}
		conn := newBufferedConn(netConn, c.cfg.ProbeBufferSize)
		if isTCPProbe(ctx, conn) {
			log.G(ctx).Debug("detected TCP kube-probe, ignoring connection")
			return conn, false, nil
		}
		if isHTTPProbe(ctx, conn) {
			log.G(ctx).Debug("detected HTTP kube-probe request, responding")
			if err := probeResponse(conn); err != nil {
				log.G(ctx).Errorf("responding to kube-probe: %s", err)
			}
			return conn, false, nil
		}
		return conn, true, nil
	}
}

func isKubeletAddr(ctx context.Context, remoteAddr net.Addr, act activator.Activator) bool {
	srv, ok := act.(*activator.Server)
	if !ok {
		return false
	}
	tcpAddr, ok := remoteAddr.(*net.TCPAddr)
	if !ok {
		log.G(ctx).Debugf("remoteAddr is not a *net.TCPAddr: %T", remoteAddr)
		return false
	}
	remoteIPAddr := tcpAddr.AddrPort().Addr().Unmap()
	kubeletAddr, err := srv.GetKubeletAddr(remoteIPAddr.Is6())
	if err != nil {
		log.G(ctx).WithError(err).Debug("getting kubelet addr")
		return false
	}
	if kubeletAddr.Unmap().Compare(remoteIPAddr) == 0 {
		return true
	}
	log.G(ctx).Debugf("remote addr %s does not match kubelet addr %s", remoteIPAddr, kubeletAddr.Unmap().String())
	return false
}

// isTCPProbe detects a TCP probe. It peeks 1 byte into the connection and if it
// receives an immediate [io.EOF] we know the conn has already been closed
// without receiving a single byte. Even if it wasn't a kube-probe, it's
// probably fine to not restore the application in case a connection is closed
// without receiving a single byte. If kubernetes ever starts to send something,
// this would need to be reworked but should be caught by e2e tests.
// https://github.com/kubernetes/kubernetes/blob/7cc3faf39d89d11c910db9ad19adfd931250e01c/pkg/probe/tcp/tcp.go#L50
func isTCPProbe(ctx context.Context, conn bufConn) bool {
	_, err := conn.Peek(1)
	return err == io.EOF
}

// isHTTPProbe detects an HTTP probe by parsing the request line and headers
// from the conn buffer. If it's a valid HTTP request, we simply read the user
// agent string and assume it's a probe if it has the prefix "kube-probe/".
// net/http is avoided on purpose as it adds a lot to the shim binary size.
func isHTTPProbe(ctx context.Context, conn bufConn) bool {
	if _, err := conn.Peek(1); err != nil {
		return false
	}
	b, err := conn.Peek(min(conn.r.Buffered(), conn.r.Size()))
	if err != nil && err != io.EOF {
		log.G(ctx).WithError(err).Debug("peek")
		return false
	}
	tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(b)))
	line, err := tp.ReadLine()
	if err != nil {
		log.G(ctx).WithError(err).Debug("reading request line")
		return false
	}
	if parts := strings.Fields(line); len(parts) != 3 || !strings.HasPrefix(parts[2], "HTTP/") {
		return false
	}
	header, err := tp.ReadMIMEHeader()
	if err != nil {
		log.G(ctx).WithError(err).Debug("reading request header")
		return false
	}
	return strings.HasPrefix(header.Get("User-Agent"), "kube-probe/")
}

// probeResponse writes an HTTP response to conn that satisfies the kubelet. It
// uses the status code 204 as the response does not contain a body and it
// makes it simpler to detect a probe response in testing.
func probeResponse(conn net.Conn) error {
	_, err := io.WriteString(conn, "HTTP/1.1 204 No Content\r\n\r\n")
	return err
}

type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func newBufferedConn(c net.Conn, size int) bufConn {
	return bufConn{c, bufio.NewReaderSize(c, size)}
}

func (b bufConn) Peek(n int) ([]byte, error) {
	return b.r.Peek(n)
}

func (b bufConn) Read(p []byte) (int, error) {
	return b.r.Read(p)
}
