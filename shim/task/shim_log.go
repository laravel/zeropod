//go:build linux

package task

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/log"
	"golang.org/x/sys/unix"
)

const shimLogFifo = "log"

// shimLog moves the shim log to the fifo of a remaining task. The shim only
// writes to the log fifo of the bundle it was started in, which with shim
// grouping can be deleted while other tasks are still running.
type shimLog struct {
	mu     sync.Mutex
	stderr *os.File
}

func newShimLog() *shimLog {
	return &shimLog{stderr: os.Stderr}
}

func (l *shimLog) moveFrom(deleted string, candidates []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.inUse(deleted) {
		return nil
	}
	var errs []error
	for _, bundle := range candidates {
		if bundle == deleted {
			continue
		}
		err := l.moveTo(bundle)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (l *shimLog) inUse(bundle string) bool {
	var current, fifo unix.Stat_t
	if err := unix.Fstat(int(l.stderr.Fd()), &current); err != nil || current.Nlink == 0 {
		// the fifo we write to is already unlinked, take any bundle to recover.
		return true
	}
	if err := unix.Stat(filepath.Join(bundle, shimLogFifo), &fifo); err != nil {
		return false
	}
	return current.Dev == fifo.Dev && current.Ino == fifo.Ino
}

func (l *shimLog) moveTo(bundle string) error {
	fd, err := unix.Open(filepath.Join(bundle, shimLogFifo), unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("opening shim log fifo of %s: %w", bundle, err)
	}
	//nolint:errcheck
	defer unix.Close(fd)
	if err := unix.SetNonblock(fd, false); err != nil {
		return fmt.Errorf("setting shim log fifo to blocking: %w", err)
	}
	if err := unix.Dup3(fd, int(l.stderr.Fd()), 0); err != nil {
		return fmt.Errorf("duplicating shim log fifo onto stderr: %w", err)
	}
	// the containerd shim initially sets the logger output to its own fifo,
	// which still points to the old bundle.
	logger := log.L.Logger
	old := logger.Out
	logger.SetOutput(l.stderr)
	if c, ok := old.(io.Closer); ok && old != io.Writer(l.stderr) {
		if err := c.Close(); err != nil {
			log.L.WithError(err).Warn("closing previous shim log")
		}
	}
	return nil
}
