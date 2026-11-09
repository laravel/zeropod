//go:build linux

package task

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/fifo"
	"github.com/containerd/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type testBundle struct {
	path   string
	reader *bufio.Reader
	file   *os.File
}

func newTestBundle(t *testing.T) *testBundle {
	t.Helper()
	path := t.TempDir()
	require.NoError(t, unix.Mkfifo(filepath.Join(path, shimLogFifo), 0o700))
	fd, err := unix.Open(filepath.Join(path, shimLogFifo), unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	require.NoError(t, err)
	f := os.NewFile(uintptr(fd), "reader")
	return &testBundle{path: path, file: f, reader: bufio.NewReader(f)}
}

func (b *testBundle) readLine(t *testing.T) string {
	t.Helper()
	require.NoError(t, b.file.SetReadDeadline(time.Now().Add(5*time.Second)))
	line, err := b.reader.ReadString('\n')
	require.NoError(t, err)
	return line
}

func (b *testBundle) close(t *testing.T) {
	assert.NoError(t, b.file.Close())
	assert.NoError(t, os.RemoveAll(b.path))
}

func countFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

// setupShimLog sets up the logger like the containerd shim does on startup.
func setupShimLog(t *testing.T, initial *testBundle) *shimLog {
	t.Helper()
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = devNull.Close() })

	f, err := fifo.OpenFifoDup2(context.Background(), filepath.Join(initial.path, shimLogFifo), unix.O_WRONLY, 0o700, int(devNull.Fd()))
	require.NoError(t, err)

	logger := log.L.Logger
	out := logger.Out
	logger.SetOutput(f)
	t.Cleanup(func() { logger.SetOutput(out) })

	return &shimLog{stderr: devNull}
}

func TestShimLogMove(t *testing.T) {
	first, second := newTestBundle(t), newTestBundle(t)
	defer first.close(t)
	defer second.close(t)
	l := setupShimLog(t, first)

	log.L.Info("before move")
	assert.Contains(t, first.readLine(t), "before move")

	require.NoError(t, l.moveFrom(second.path, []string{first.path}))
	assert.True(t, l.inUse(first.path))

	noReader := t.TempDir()
	require.NoError(t, unix.Mkfifo(filepath.Join(noReader, shimLogFifo), 0o700))

	require.NoError(t, l.moveFrom(first.path, []string{first.path, noReader, second.path}))
	assert.True(t, l.inUse(second.path))
	assert.False(t, l.inUse(first.path))

	log.L.Info("after move")
	assert.Contains(t, second.readLine(t), "after move")
	_, err := l.stderr.WriteString("raw stderr\n")
	require.NoError(t, err)
	assert.Equal(t, "raw stderr\n", second.readLine(t))

	require.NoError(t, os.Remove(filepath.Join(second.path, shimLogFifo)))
	assert.True(t, l.inUse(first.path))
}

func TestShimLogMoveNoFDLeak(t *testing.T) {
	current := newTestBundle(t)
	l := setupShimLog(t, current)

	cycle := func(i int) {
		next := newTestBundle(t)
		require.NoError(t, l.moveFrom(current.path, []string{next.path}))
		log.L.Infof("cycle %d", i)
		assert.True(t, strings.Contains(next.readLine(t), "cycle"))
		current.close(t)
		current = next
	}
	// the first move closes the initial fifo, so count after it.
	cycle(0)
	before := countFDs(t)
	for i := 1; i <= 100; i++ {
		cycle(i)
	}
	assert.Equal(t, before, countFDs(t))
	current.close(t)
}
