package task

import (
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/cmd/containerd-shim-runc-v2/process"
	"github.com/containerd/containerd/v2/cmd/containerd-shim-runc-v2/runc"
	zshim "github.com/laravel/zeropod/shim"
)

func TestPreventExitBeforeRegistration(t *testing.T) {
	container := &runc.Container{ID: "starting"}
	p := &earlyExitProcess{id: container.ID, pid: 42}
	cp := containerProcess{Container: container, Process: p}
	w := &wrapper{
		zeropodContainers: map[string]*zshim.Container{
			// Create publishes the Zeropod container before Start calls Register.
			// Its process references are still nil when an early exit arrives.
			container.ID: {
				CheckpointRestore: &sync.Mutex{},
			},
		},
	}

	if w.preventExit(cp) {
		t.Fatal("exit before registration must reach the task service")
	}
}

// Only identity is needed by the exit interceptor. Embedding the interface makes
// unexpected process operations fail instead of silently accepting them.
type earlyExitProcess struct {
	process.Process
	id  string
	pid int
}

func (p *earlyExitProcess) ID() string { return p.id }
func (p *earlyExitProcess) Pid() int   { return p.pid }
