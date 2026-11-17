package kv

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestTxnCompareAndSwapSuccess(t *testing.T) {
	engine := NewMVCCEngine()

	rev1, _ := engine.Put("/pods/nginx", []byte("v1"))

	txn := &Txn{
		If: []Compare{
			{
				Key:      "/pods/nginx",
				Target:   CompareModRev,
				Operator: "==",
				Value:    rev1,
			},
		},
		Then: []Op{
			{Type: "PUT", Key: "/pods/nginx", Value: []byte("v2")},
			{Type: "PUT", Key: "/pods/status", Value: []byte("running")},
		},
		Else: []Op{
			{Type: "PUT", Key: "/pods/conflict", Value: []byte("true")},
		},
	}

	resp := engine.Txn(txn)
	if !resp.Success {
		t.Fatalf("expected txn to succeed")
	}
	if resp.Rev != 2 {
		t.Fatalf("expected revision 2, got %d", resp.Rev)
	}

	recNginx, ok := engine.Get("/pods/nginx")
	if !ok || !bytes.Equal(recNginx.Value, []byte("v2")) || recNginx.ModifiedRev != 2 {
		t.Fatalf("unexpected nginx record: %+v", recNginx)
	}

	recStatus, ok := engine.Get("/pods/status")
	if !ok || !bytes.Equal(recStatus.Value, []byte("running")) || recStatus.ModifiedRev != 2 {
		t.Fatalf("unexpected status record: %+v", recStatus)
	}

	if _, ok := engine.Get("/pods/conflict"); ok {
		t.Fatalf("expected else branch not to run")
	}
}

func TestTxnCompareAndSwapFailure(t *testing.T) {
	engine := NewMVCCEngine()

	engine.Put("/pods/nginx", []byte("v1"))

	txn := &Txn{
		If: []Compare{
			{
				Key:      "/pods/nginx",
				Target:   CompareModRev,
				Operator: "==",
				Value:    999,
			},
		},
		Then: []Op{
			{Type: "PUT", Key: "/pods/nginx", Value: []byte("v2")},
		},
		Else: []Op{
			{Type: "GET", Key: "/pods/nginx"},
		},
	}

	resp := engine.Txn(txn)
	if resp.Success {
		t.Fatalf("expected txn to fail")
	}
	if resp.Rev != 1 {
		t.Fatalf("expected revision to stay 1, got %d", resp.Rev)
	}
	if len(resp.Responses) != 1 || !bytes.Equal(resp.Responses[0].Record.Value, []byte("v1")) {
		t.Fatalf("expected else branch to return current v1 record, got %+v", resp.Responses)
	}
}

func TestTxnVersionAndValueComparisons(t *testing.T) {
	engine := NewMVCCEngine()

	engine.Put("/test", []byte("alpha"))
	engine.Put("/test", []byte("beta"))

	txn := &Txn{
		If: []Compare{
			{Key: "/test", Target: CompareVersion, Operator: "==", Value: 2},
			{Key: "/test", Target: CompareValue, Operator: "==", Value: "beta"},
		},
		Then: []Op{
			{Type: "PUT", Key: "/test", Value: []byte("gamma")},
		},
	}

	resp := engine.Txn(txn)
	if !resp.Success {
		t.Fatalf("expected txn to succeed")
	}

	rec, ok := engine.Get("/test")
	if !ok || string(rec.Value) != "gamma" {
		t.Fatalf("expected gamma, got %+v", rec)
	}
}

func TestKVStoreApplyTxn(t *testing.T) {
	store := NewKVStore()

	cmdPut, _ := json.Marshal(Command{Op: "PUT", Key: "count", Value: "10"})
	store.Apply(cmdPut)

	txnJSON, _ := json.Marshal(Txn{
		If: []Compare{
			{Key: "count", Target: CompareVersion, Operator: "==", Value: 1},
		},
		Then: []Op{
			{Type: "PUT", Key: "count", Value: []byte("11")},
		},
	})

	cmdTxn, _ := json.Marshal(Command{Op: "TXN", Value: string(txnJSON)})
	result := store.Apply(cmdTxn)

	resp, ok := result.(*TxnResponse)
	if !ok || !resp.Success {
		t.Fatalf("expected successful TxnResponse, got %v", result)
	}

	val, ok := store.Get("count")
	if !ok || string(val.Value) != "11" {
		t.Fatalf("expected count=11, got %v (ok=%v)", val, ok)
	}
}
