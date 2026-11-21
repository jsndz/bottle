package controllerruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockReconciler struct {
	mu        sync.Mutex
	calls     map[string]int
	reconcile func(ctx context.Context, req Request) (Result, error)
}

func (m *mockReconciler) Reconcile(ctx context.Context, req Request) (Result, error) {
	m.mu.Lock()
	if m.calls == nil {
		m.calls = make(map[string]int)
	}
	m.calls[req.Namespace+"/"+req.Name]++
	fn := m.reconcile
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}
	return Result{}, nil
}

func TestParseKey(t *testing.T) {
	cases := []struct {
		in        string
		expected  Request
	}{
		{
			in:       "/registry/pods/default/nginx",
			expected: Request{Namespace: "default", Name: "nginx"},
		},
		{
			in:       "/registry/nodes/worker-1",
			expected: Request{Namespace: "", Name: "worker-1"},
		},
		{
			in:       "kube-system/coredns",
			expected: Request{Namespace: "kube-system", Name: "coredns"},
		},
		{
			in:       "worker-2",
			expected: Request{Namespace: "", Name: "worker-2"},
		},
		{
			in:       "",
			expected: Request{Namespace: "", Name: ""},
		},
	}

	for _, c := range cases {
		got := parseKey(c.in)
		if got != c.expected {
			t.Errorf("parseKey(%q) = %+v, want %+v", c.in, got, c.expected)
		}
	}
}

func TestControllerReconcile(t *testing.T) {
	rec := &mockReconciler{}
	c := NewController("test-controller", "pods", "", nil, rec)
	defer c.Stop()

	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	c.Queue().Add("default/pod-1")

	time.Sleep(50 * time.Millisecond)

	rec.mu.Lock()
	calls := rec.calls["default/pod-1"]
	rec.mu.Unlock()

	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestControllerRequeueOnError(t *testing.T) {
	var count int32
	rec := &mockReconciler{
		reconcile: func(ctx context.Context, req Request) (Result, error) {
			val := atomic.AddInt32(&count, 1)
			if val == 1 {
				return Result{}, errors.New("simulated error")
			}
			return Result{}, nil
		},
	}

	c := NewController("test-retry", "pods", "", nil, rec)
	defer c.Stop()

	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	c.Queue().Add("default/pod-err")

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) < 2 {
		t.Fatalf("expected at least 2 reconciliations on error retry, got %d", atomic.LoadInt32(&count))
	}
}

func TestControllerRequeueAfter(t *testing.T) {
	var count int32
	rec := &mockReconciler{
		reconcile: func(ctx context.Context, req Request) (Result, error) {
			val := atomic.AddInt32(&count, 1)
			if val == 1 {
				return Result{RequeueAfter: 30 * time.Millisecond}, nil
			}
			return Result{}, nil
		},
	}

	c := NewController("test-delayed", "pods", "", nil, rec)
	defer c.Stop()

	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	c.Queue().Add("default/pod-delayed")

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) < 2 {
		t.Fatalf("expected at least 2 reconciliations with RequeueAfter, got %d", atomic.LoadInt32(&count))
	}
}

func TestControllerConcurrentWorkers(t *testing.T) {
	var active int32
	var maxActive int32
	var completed int32

	rec := &mockReconciler{
		reconcile: func(ctx context.Context, req Request) (Result, error) {
			cur := atomic.AddInt32(&active, 1)
			for {
				prev := atomic.LoadInt32(&maxActive)
				if cur <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&active, -1)
			atomic.AddInt32(&completed, 1)
			return Result{}, nil
		},
	}

	c := NewController("test-concurrent", "pods", "", nil, rec)
	c.SetWorkers(4)
	defer c.Stop()

	if err := c.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	c.Queue().Add("default/pod-1")
	c.Queue().Add("default/pod-2")
	c.Queue().Add("default/pod-3")
	c.Queue().Add("default/pod-4")

	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt32(&completed) != 4 {
		t.Fatalf("expected 4 completed, got %d", atomic.LoadInt32(&completed))
	}
	if atomic.LoadInt32(&maxActive) < 2 {
		t.Fatalf("expected concurrent worker execution, maxActive=%d", atomic.LoadInt32(&maxActive))
	}
}
