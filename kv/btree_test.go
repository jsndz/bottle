package kv

import (
	"fmt"
	"testing"
)

func TestBTreeBasic(t *testing.T) {
	tree := NewBTree(4)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		tree.Put(key, i)
	}

	if tree.Len() != 100 {
		t.Fatalf("expected 100 items, got %d", tree.Len())
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val, ok := tree.Get(key)
		if !ok || val != i {
			t.Fatalf("expected %d for %s, got %v (ok=%v)", i, key, val, ok)
		}
	}

	tree.Put("key-050", 999)
	val, ok := tree.Get("key-050")
	if !ok || val != 999 {
		t.Fatalf("expected 999 for key-050, got %v", val)
	}

	_, ok = tree.Get("nonexistent")
	if ok {
		t.Fatalf("expected nonexistent to return false")
	}
}

func TestBTreePrefixAndRange(t *testing.T) {
	tree := NewBTree(4)

	tree.Put("/pods/nginx-1", "pod1")
	tree.Put("/pods/nginx-2", "pod2")
	tree.Put("/pods/redis", "pod3")
	tree.Put("/nodes/node-1", "node1")
	tree.Put("/services/web", "svc1")

	var pods []string
	tree.AscendPrefix("/pods/", func(key string, val any) bool {
		pods = append(pods, key)
		return true
	})

	if len(pods) != 3 {
		t.Fatalf("expected 3 pods, got %d: %v", len(pods), pods)
	}
	if pods[0] != "/pods/nginx-1" || pods[1] != "/pods/nginx-2" || pods[2] != "/pods/redis" {
		t.Fatalf("unexpected pods order: %v", pods)
	}

	var rangeKeys []string
	tree.AscendRange("/pods/nginx-2", "/services/web", func(key string, val any) bool {
		rangeKeys = append(rangeKeys, key)
		return true
	})

	if len(rangeKeys) != 2 || rangeKeys[0] != "/pods/nginx-2" || rangeKeys[1] != "/pods/redis" {
		t.Fatalf("unexpected range keys: %v", rangeKeys)
	}
}

func TestBTreeDelete(t *testing.T) {
	tree := NewBTree(4)

	for i := 0; i < 50; i++ {
		tree.Put(fmt.Sprintf("k-%02d", i), i)
	}

	for i := 0; i < 50; i += 2 {
		deleted := tree.Delete(fmt.Sprintf("k-%02d", i))
		if !deleted {
			t.Fatalf("failed to delete k-%02d", i)
		}
	}

	if tree.Len() != 25 {
		t.Fatalf("expected 25 items left, got %d", tree.Len())
	}

	for i := 1; i < 50; i += 2 {
		val, ok := tree.Get(fmt.Sprintf("k-%02d", i))
		if !ok || val != i {
			t.Fatalf("expected %d for k-%02d, got %v", i, i, val)
		}
	}
}
