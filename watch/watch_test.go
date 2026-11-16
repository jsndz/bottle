package watch

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWatchExactAndPrefixMatching(t *testing.T) {
	hub := NewWatchHub()
	defer hub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wPods := hub.Watch(ctx, "/pods/")
	wNodes := hub.Watch(ctx, "/nodes/")
	wNginx := hub.Watch(ctx, "/pods/nginx")

	hub.Publish(NewEvent(PUT, "/pods/nginx", []byte("running"), nil, 1))

	select {
	case ev := <-wPods.Buffer:
		if ev.Key != "/pods/nginx" || string(ev.Value) != "running" {
			t.Fatalf("unexpected event on wPods: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event on wPods")
	}

	select {
	case ev := <-wNginx.Buffer:
		if ev.Key != "/pods/nginx" {
			t.Fatalf("unexpected event on wNginx: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event on wNginx")
	}

	select {
	case ev := <-wNodes.Buffer:
		t.Fatalf("wNodes should not receive pod event: %+v", ev)
	case <-time.After(20 * time.Millisecond):
	}

	hub.Publish(NewEvent(PUT, "/nodes/worker-1", []byte("ready"), nil, 2))

	select {
	case ev := <-wNodes.Buffer:
		if ev.Key != "/nodes/worker-1" {
			t.Fatalf("unexpected event on wNodes: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event on wNodes")
	}

	select {
	case ev := <-wPods.Buffer:
		t.Fatalf("wPods should not receive node event: %+v", ev)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestWatchEventContent(t *testing.T) {
	hub := NewWatchHub()
	defer hub.Close()

	w := hub.Watch(context.Background(), "/data")

	hub.Publish(NewEvent(PUT, "/data", []byte("v1"), nil, 10))
	hub.Publish(NewEvent(PUT, "/data", []byte("v2"), []byte("v1"), 11))
	hub.Publish(NewEvent(DELETE, "/data", nil, []byte("v2"), 12))

	ev1 := <-w.Buffer
	if ev1.Type != PUT || string(ev1.Value) != "v1" || ev1.PrevValue != nil || ev1.Rev != 10 {
		t.Fatalf("unexpected ev1: %+v", ev1)
	}

	ev2 := <-w.Buffer
	if ev2.Type != PUT || string(ev2.Value) != "v2" || string(ev2.PrevValue) != "v1" || ev2.Rev != 11 {
		t.Fatalf("unexpected ev2: %+v", ev2)
	}

	ev3 := <-w.Buffer
	if ev3.Type != DELETE || ev3.Value != nil || string(ev3.PrevValue) != "v2" || ev3.Rev != 12 {
		t.Fatalf("unexpected ev3: %+v", ev3)
	}
}

func TestWatchContextCancellationAndUnwatch(t *testing.T) {
	hub := NewWatchHub()
	defer hub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	w1 := hub.Watch(ctx, "/test1")
	w2 := hub.Watch(context.Background(), "/test2")

	cancel()

	time.Sleep(50 * time.Millisecond)

	hub.mu.RLock()
	_, exists1 := hub.Watchers[w1.ID]
	hub.mu.RUnlock()
	if exists1 {
		t.Fatal("expected w1 to be removed after cancel")
	}

	_, ok := <-w1.Buffer
	if ok {
		t.Fatal("expected w1.Buffer to be closed")
	}

	hub.Unwatch(w2.ID)

	hub.mu.RLock()
	_, exists2 := hub.Watchers[w2.ID]
	hub.mu.RUnlock()
	if exists2 {
		t.Fatal("expected w2 to be removed after unwatch")
	}

	_, ok = <-w2.Buffer
	if ok {
		t.Fatal("expected w2.Buffer to be closed")
	}
}

func TestWatchSlowConsumerNonBlocking(t *testing.T) {
	hub := NewWatchHub()
	defer hub.Close()

	wSlow := &Watcher{
		ID:     99,
		Prefix: "/fast",
		Buffer: make(chan Event, 2),
	}
	hub.mu.Lock()
	hub.Watchers[wSlow.ID] = wSlow
	hub.mu.Unlock()

	for i := 0; i < 10; i++ {
		hub.Publish(NewEvent(PUT, "/fast/item", []byte("val"), nil, int64(i+1)))
	}

	hub.mu.RLock()
	_, exists := hub.Watchers[wSlow.ID]
	hub.mu.RUnlock()

	if exists {
		t.Fatal("expected slow watcher to be evicted")
	}
}

func TestWatchHubClose(t *testing.T) {
	hub := NewWatchHub()
	w1 := hub.Watch(context.Background(), "/a")
	w2 := hub.Watch(context.Background(), "/b")

	hub.Close()

	if len(hub.Watchers) != 0 {
		t.Fatalf("expected 0 watchers after Close, got %d", len(hub.Watchers))
	}

	_, ok1 := <-w1.Buffer
	_, ok2 := <-w2.Buffer
	if ok1 || ok2 {
		t.Fatal("expected all watcher channels to be closed")
	}
}

func TestWatchConcurrency(t *testing.T) {
	hub := NewWatchHub()
	defer hub.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := hub.Watch(ctx, "/concurrent/")
			for j := 0; j < 50; j++ {
				hub.Publish(NewEvent(PUT, "/concurrent/test", []byte("ok"), nil, int64(j+1)))
			}
			hub.Unwatch(w.ID)
		}(i)
	}
	wg.Wait()
}
