package kv

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/jsndz/bottle/watch"
)

func TestKVStoreApplyAndGet(t *testing.T) {
	store := NewKVStore()

	putCmd, _ := json.Marshal(Command{Op: "PUT", Key: "hello", Value: "world"})
	store.Apply(putCmd)

	val, ok := store.Get("hello")
	if !ok || val != "world" {
		t.Fatalf("expected world, got %q (ok=%v)", val, ok)
	}

	delCmd, _ := json.Marshal(Command{Op: "DELETE", Key: "hello"})
	store.Apply(delCmd)

	_, ok = store.Get("hello")
	if ok {
		t.Fatalf("expected key to be deleted")
	}
}

func TestKVStoreConcurrent(t *testing.T) {
	store := NewKVStore()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd, _ := json.Marshal(Command{Op: "PUT", Key: "k", Value: "v"})
			store.Apply(cmd)
			store.Get("k")
		}(i)
	}

	wg.Wait()
}

func TestKVStoreWatchIntegration(t *testing.T) {
	store := NewKVStore()
	hub := watch.NewWatchHub()
	defer hub.Close()

	store.SetEventCallback(func(e watch.Event) {
		hub.Publish(e)
	})

	w := hub.Watch(context.Background(), "/pods/")

	cmdPut1, _ := json.Marshal(Command{Op: "PUT", Key: "/pods/web", Value: "nginx-1"})
	store.Apply(cmdPut1)

	select {
	case ev := <-w.Buffer:
		if ev.Type != watch.PUT || ev.Key != "/pods/web" || string(ev.Value) != "nginx-1" || ev.PrevValue != nil {
			t.Fatalf("unexpected put event: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for put event")
	}

	cmdPut2, _ := json.Marshal(Command{Op: "PUT", Key: "/pods/web", Value: "nginx-2"})
	store.Apply(cmdPut2)

	select {
	case ev := <-w.Buffer:
		if ev.Type != watch.PUT || string(ev.Value) != "nginx-2" || string(ev.PrevValue) != "nginx-1" {
			t.Fatalf("unexpected update event: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for update event")
	}

	cmdDel, _ := json.Marshal(Command{Op: "DELETE", Key: "/pods/web"})
	store.Apply(cmdDel)

	select {
	case ev := <-w.Buffer:
		if ev.Type != watch.DELETE || ev.Value != nil || string(ev.PrevValue) != "nginx-2" {
			t.Fatalf("unexpected delete event: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for delete event")
	}

	txn := Txn{
		Then: []Op{
			{Type: "PUT", Key: "/pods/app-1", Value: []byte("val-1")},
			{Type: "PUT", Key: "/pods/app-2", Value: []byte("val-2")},
		},
	}
	txnBytes, _ := json.Marshal(txn)
	cmdTxn, _ := json.Marshal(Command{Op: "TXN", Value: string(txnBytes)})
	store.Apply(cmdTxn)

	evA := <-w.Buffer
	evB := <-w.Buffer

	if evA.Key != "/pods/app-1" || evB.Key != "/pods/app-2" {
		t.Fatalf("unexpected txn event order: %s, %s", evA.Key, evB.Key)
	}
	if evA.Rev != evB.Rev {
		t.Fatalf("expected txn events to share same revision: %d != %d", evA.Rev, evB.Rev)
	}
}
