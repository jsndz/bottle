package controllerruntime

import (
	"sync"
	"testing"
	"time"
)

func TestWorkQueueAddGetDone(t *testing.T) {
	q := NewWorkQueue()
	q.Add("foo")
	q.Add("foo")
	if q.Len() != 1 {
		t.Fatalf("expected len 1, got %d", q.Len())
	}

	key, shutdown := q.Get()
	if shutdown || key != "foo" {
		t.Fatalf("unexpected get: %s, %v", key, shutdown)
	}

	q.Add("foo")
	if q.Len() != 0 {
		t.Fatalf("expected len 0 while processing, got %d", q.Len())
	}

	q.Done(key)
	if q.Len() != 1 {
		t.Fatalf("expected len 1 after done of dirty item, got %d", q.Len())
	}

	key, shutdown = q.Get()
	if shutdown || key != "foo" {
		t.Fatalf("unexpected get: %s, %v", key, shutdown)
	}
	q.Done(key)
	if q.Len() != 0 {
		t.Fatalf("expected len 0, got %d", q.Len())
	}
}

func TestWorkQueueShutdown(t *testing.T) {
	q := NewWorkQueue()
	doneCh := make(chan struct{})

	go func() {
		key, shutdown := q.Get()
		if !shutdown || key != "" {
			t.Errorf("expected shutdown true, got key=%s, shutdown=%v", key, shutdown)
		}
		close(doneCh)
	}()

	time.Sleep(20 * time.Millisecond)
	q.ShutDown()

	select {
	case <-doneCh:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Get after ShutDown")
	}

	if !q.ShuttingDown() {
		t.Fatal("expected ShuttingDown to be true")
	}

	q.Add("bar")
	if q.Len() != 0 {
		t.Fatal("expected Add after ShutDown to be ignored")
	}
}

func TestWorkQueueAddAfter(t *testing.T) {
	q := NewWorkQueue()
	start := time.Now()
	q.AddAfter("delayed", 50*time.Millisecond)

	key, shutdown := q.Get()
	if shutdown || key != "delayed" {
		t.Fatalf("unexpected get: %s, %v", key, shutdown)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatalf("item popped too early: %v", time.Since(start))
	}
	q.Done(key)
}

func TestWorkQueueRateLimited(t *testing.T) {
	q := NewWorkQueue()
	q.AddRateLimited("fail-key")
	if q.NumRequeues("fail-key") != 1 {
		t.Fatalf("expected 1 requeue, got %d", q.NumRequeues("fail-key"))
	}

	key, shutdown := q.Get()
	if shutdown || key != "fail-key" {
		t.Fatalf("unexpected get: %s, %v", key, shutdown)
	}
	q.Done(key)

	q.AddRateLimited("fail-key")
	if q.NumRequeues("fail-key") != 2 {
		t.Fatalf("expected 2 requeues, got %d", q.NumRequeues("fail-key"))
	}

	q.Forget("fail-key")
	if q.NumRequeues("fail-key") != 0 {
		t.Fatalf("expected 0 requeues after Forget, got %d", q.NumRequeues("fail-key"))
	}
}

func TestWorkQueueConcurrency(t *testing.T) {
	q := NewWorkQueue()
	var wg sync.WaitGroup
	itemCount := 100

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				key, shutdown := q.Get()
				if shutdown {
					return
				}
				time.Sleep(1 * time.Millisecond)
				q.Done(key)
			}
		}()
	}

	for i := 0; i < itemCount; i++ {
		q.Add("item")
	}

	time.Sleep(50 * time.Millisecond)
	q.ShutDown()
	wg.Wait()
}
