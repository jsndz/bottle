package kv

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestMVCCBasicPutGetUpdate(t *testing.T) {
	engine := NewMVCCEngine()

	rev1, rec1 := engine.Put("/pods/nginx", []byte("v1"))
	if rev1 != 1 {
		t.Fatalf("expected rev 1, got %d", rev1)
	}
	if rec1.CreatedRev != 1 || rec1.ModifiedRev != 1 || rec1.Version != 1 || rec1.PrevRev != 0 {
		t.Fatalf("unexpected rec1 metadata: %+v", rec1)
	}

	got, ok := engine.Get("/pods/nginx")
	if !ok || !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("expected v1, got %s (ok=%v)", got.Value, ok)
	}

	rev2, rec2 := engine.Put("/pods/nginx", []byte("v2"))
	if rev2 != 2 {
		t.Fatalf("expected rev 2, got %d", rev2)
	}
	if rec2.CreatedRev != 1 {
		t.Fatalf("expected CreatedRev 1, got %d", rec2.CreatedRev)
	}
	if rec2.ModifiedRev != 2 {
		t.Fatalf("expected ModifiedRev 2, got %d", rec2.ModifiedRev)
	}
	if rec2.Version != 2 {
		t.Fatalf("expected Version 2, got %d", rec2.Version)
	}
	if rec2.PrevRev != 1 {
		t.Fatalf("expected PrevRev 1, got %d", rec2.PrevRev)
	}

	got2, ok2 := engine.Get("/pods/nginx")
	if !ok2 || !bytes.Equal(got2.Value, []byte("v2")) {
		t.Fatalf("expected v2, got %s (ok=%v)", got2.Value, ok2)
	}
}

func TestMVCCDeleteDoesNotCorruptHistory(t *testing.T) {
	engine := NewMVCCEngine()

	rev1, _ := engine.Put("k1", []byte("val1"))
	rev2, _ := engine.Put("k1", []byte("val2"))

	delRev, prevRec, ok := engine.Delete("k1")
	if !ok || delRev != 3 {
		t.Fatalf("expected delRev 3 (ok=true), got delRev %d (ok=%v)", delRev, ok)
	}
	if prevRec == nil || !bytes.Equal(prevRec.Value, []byte("val2")) {
		t.Fatalf("expected prevRec to be val2, got %+v", prevRec)
	}

	if _, exists := engine.Get("k1"); exists {
		t.Fatalf("expected k1 to be deleted")
	}

	hist1, ok1 := engine.Storage.Get(rev1, "k1")
	if !ok1 || hist1.IsDeleted || !bytes.Equal(hist1.Value, []byte("val1")) {
		t.Fatalf("rev1 in storage was corrupted: %+v", hist1)
	}

	hist2, ok2 := engine.Storage.Get(rev2, "k1")
	if !ok2 || hist2.IsDeleted || !bytes.Equal(hist2.Value, []byte("val2")) {
		t.Fatalf("rev2 in storage was corrupted: %+v", hist2)
	}

	tombstone, ok3 := engine.Storage.Get(delRev, "k1")
	if !ok3 || !tombstone.IsDeleted || tombstone.Value != nil || tombstone.PrevRev != rev2 {
		t.Fatalf("invalid tombstone at rev 3: %+v", tombstone)
	}
}

func TestMVCCGetAtRevision(t *testing.T) {
	engine := NewMVCCEngine()

	engine.Put("k1", []byte("k1_v1"))
	engine.Put("k1", []byte("k1_v2"))
	engine.Put("k2", []byte("k2_v1"))
	engine.Put("k1", []byte("k1_v3"))

	r1, ok := engine.GetAtRevision("k1", 1)
	if !ok || string(r1.Value) != "k1_v1" {
		t.Fatalf("expected k1_v1 at rev 1, got %v (ok=%v)", r1, ok)
	}

	r2, ok := engine.GetAtRevision("k1", 2)
	if !ok || string(r2.Value) != "k1_v2" {
		t.Fatalf("expected k1_v2 at rev 2, got %v (ok=%v)", r2, ok)
	}

	r3, ok := engine.GetAtRevision("k1", 3)
	if !ok || string(r3.Value) != "k1_v2" {
		t.Fatalf("expected k1_v2 at rev 3, got %v (ok=%v)", r3, ok)
	}

	r4, ok := engine.GetAtRevision("k1", 4)
	if !ok || string(r4.Value) != "k1_v3" {
		t.Fatalf("expected k1_v3 at rev 4, got %v (ok=%v)", r4, ok)
	}

	rFuture, ok := engine.GetAtRevision("k1", 999)
	if !ok || string(rFuture.Value) != "k1_v3" {
		t.Fatalf("expected k1_v3 at future rev, got %v (ok=%v)", rFuture, ok)
	}

	_, ok0 := engine.GetAtRevision("k1", 0)
	if ok0 {
		t.Fatalf("expected not found at rev 0")
	}

	_, okK2Before := engine.GetAtRevision("k2", 2)
	if okK2Before {
		t.Fatalf("expected k2 not found at rev 2")
	}

	rk2, okK2 := engine.GetAtRevision("k2", 3)
	if !okK2 || string(rk2.Value) != "k2_v1" {
		t.Fatalf("expected k2_v1 at rev 3, got %v (ok=%v)", rk2, okK2)
	}
}

func TestMVCCPrefix(t *testing.T) {
	engine := NewMVCCEngine()

	engine.Put("/pods/a", []byte("pod-a"))
	engine.Put("/pods/b", []byte("pod-b"))
	engine.Put("/nodes/1", []byte("node-1"))

	pods := engine.GetPrefix("/pods/")
	if len(pods) != 2 {
		t.Fatalf("expected 2 pods, got %d", len(pods))
	}

	engine.Delete("/pods/a")

	podsAfter := engine.GetPrefix("/pods/")
	if len(podsAfter) != 1 || string(podsAfter[0].Value) != "pod-b" {
		t.Fatalf("expected 1 pod (pod-b) after delete, got %v", podsAfter)
	}
}

func TestMVCCConcurrent(t *testing.T) {
	engine := NewMVCCEngine()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("/key-%d", id%5)
			engine.Put(key, []byte(fmt.Sprintf("val-%d", id)))
			engine.Get(key)
			engine.GetPrefix("/key-")
		}(i)
	}

	wg.Wait()
}
