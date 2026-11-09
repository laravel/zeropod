package shim

import (
	"context"
	"os"

	"github.com/containerd/fifo"
	"github.com/containerd/log"
	"golang.org/x/sys/unix"
)

// WithContainerLogger adds the container ID to the logger of the context.
func WithContainerLogger(ctx context.Context, id string) context.Context {
	return log.WithLogger(ctx, log.G(ctx).WithField("container", id))
}

// holdLogFIFOs opens an additional write end on the stdout/stderr fifos of the
// container while it is scaled down. Once the process exits on scale down,
// the shim closes its write end of the fifos which would cause CRI to see an
// EOF and stop logging. By holding another write end, CRI keeps reading from
// the fifos until the restored process writes to them again. The hold must be
// released once the restored process is started or the container exits, as
// CRI waits for the EOF before deleting the task.
func (c *Container) holdLogFIFOs(ctx context.Context) {
	c.logFIFOsMu.Lock()
	defer c.logFIFOsMu.Unlock()
	if len(c.logFIFOs) > 0 {
		return
	}
	stdio := c.initialProcess.Stdio()
	for _, path := range []string{stdio.Stdout, stdio.Stderr} {
		if ok, _ := fifo.IsFifo(path); !ok {
			continue
		}
		fd, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			log.G(ctx).WithError(err).Warnf("holding log fifo %s, logs will be lost after restore", path)
			continue
		}
		c.logFIFOs = append(c.logFIFOs, os.NewFile(uintptr(fd), path))
	}
}

func (c *Container) ReleaseLogFIFOs() {
	c.logFIFOsMu.Lock()
	defer c.logFIFOsMu.Unlock()
	for _, f := range c.logFIFOs {
		_ = f.Close()
	}
	c.logFIFOs = nil
}
