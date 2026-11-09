package shim

import (
	"context"
	"sync"
	"testing"
	"time"

	v1 "github.com/laravel/zeropod/api/shim/v1"
)

func TestScaleDownWithExecInFlight(t *testing.T) {
	c := &Container{CheckpointRestore: &sync.Mutex{}, cfg: &v1.Config{}}

	if c.AddExec("a") {
		t.Fatal("expected container to not be scaled down")
	}
	if err := c.scaleDown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.ScaledDown() {
		t.Fatal("scaled down with exec in flight")
	}

	c.AddExec("b")
	if n := c.RemoveExec("a"); n != 1 {
		t.Fatalf("expected 1 exec in flight, got %d", n)
	}
	if n := c.RemoveExec("b"); n != 0 {
		t.Fatalf("expected 0 execs in flight, got %d", n)
	}
	if n := c.RemoveExec("b"); n != 0 {
		t.Fatalf("expected removing an unknown exec to be a no-op, got %d", n)
	}
}

func TestAddExecWaitsForCheckpoint(t *testing.T) {
	c := &Container{CheckpointRestore: &sync.Mutex{}, cfg: &v1.Config{}}

	// simulate a checkpoint in progress
	c.CheckpointRestore.Lock()
	go func() {
		time.Sleep(50 * time.Millisecond)
		c.scaledDown = true
		c.CheckpointRestore.Unlock()
	}()

	if !c.AddExec("a") {
		t.Fatal("expected exec to see the container as scaled down after the checkpoint")
	}
}
