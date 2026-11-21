package controllerruntime

import (
	"context"
	"testing"
	"time"
)

func TestManagerAddAndStart(t *testing.T) {
	mgr := NewManager("", nil)

	rec1 := &mockReconciler{}
	rec2 := &mockReconciler{}

	if err := mgr.Add(nil); err == nil {
		t.Fatal("expected error adding nil controller")
	}

	if err := mgr.AddController("ctrl1", "pods", rec1); err != nil {
		t.Fatalf("failed to add controller 1: %v", err)
	}

	c2 := NewController("ctrl2", "nodes", "", nil, rec2)
	if err := mgr.Add(c2); err != nil {
		t.Fatalf("failed to add controller 2: %v", err)
	}

	if len(mgr.Controllers()) != 2 {
		t.Fatalf("expected 2 controllers, got %d", len(mgr.Controllers()))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}

	if err := mgr.Start(ctx); err == nil {
		t.Fatal("expected error starting already started manager")
	}

	ctrls := mgr.Controllers()
	ctrls[0].Queue().Add("default/pod-1")
	ctrls[1].Queue().Add("worker-node-1")

	time.Sleep(50 * time.Millisecond)

	rec1.mu.Lock()
	calls1 := rec1.calls["default/pod-1"]
	rec1.mu.Unlock()

	rec2.mu.Lock()
	calls2 := rec2.calls["/worker-node-1"]
	rec2.mu.Unlock()

	if calls1 != 1 {
		t.Fatalf("expected 1 call on ctrl1, got %d", calls1)
	}
	if calls2 != 1 {
		t.Fatalf("expected 1 call on ctrl2, got %d", calls2)
	}

	mgr.Stop()
	mgr.Stop()

	select {
	case <-mgr.StopCh():
	default:
		t.Fatal("expected StopCh to be closed")
	}
}

func TestManagerContextCancellation(t *testing.T) {
	mgr := NewManager("", nil)
	rec := &mockReconciler{}

	if err := mgr.AddController("ctrl", "pods", rec); err != nil {
		t.Fatalf("failed to add controller: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}

	cancel()

	select {
	case <-mgr.StopCh():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected manager to stop on context cancellation")
	}
}

func TestManagerAddAfterStart(t *testing.T) {
	mgr := NewManager("", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer mgr.Stop()

	rec := &mockReconciler{}
	if err := mgr.AddController("dynamic", "pods", rec); err != nil {
		t.Fatalf("failed to add dynamic controller: %v", err)
	}

	ctrls := mgr.Controllers()
	if len(ctrls) != 1 {
		t.Fatalf("expected 1 controller, got %d", len(ctrls))
	}

	ctrls[0].Queue().Add("default/dynamic-pod")

	time.Sleep(50 * time.Millisecond)

	rec.mu.Lock()
	calls := rec.calls["default/dynamic-pod"]
	rec.mu.Unlock()

	if calls != 1 {
		t.Fatalf("expected 1 call on dynamic controller, got %d", calls)
	}
}
