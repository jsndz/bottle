package kv

import (
	"encoding/json"
	"sync"
	"testing"
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
