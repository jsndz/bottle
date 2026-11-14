package kv

import (
	"encoding/json"
	"sync"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KVStore struct {
	mu        sync.RWMutex
	Engine    *MVCCEngine
	GlobalRev int64
}

func NewKVStore() *KVStore {
	return &KVStore{
		Engine: NewMVCCEngine(),
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	rec, ok := kv.Engine.Get(key)
	if !ok {
		return "", false
	}
	return string(rec.Value), true
}

func (kv *KVStore) Apply(data []byte) any {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "PUT", "put", "SET", "set":
		rev, rec := kv.Engine.Put(cmd.Key, []byte(cmd.Value))
		kv.GlobalRev = rev
		return string(rec.Value)
	case "DELETE", "delete", "DEL", "del":
		rev, _, ok := kv.Engine.Delete(cmd.Key)
		if ok {
			kv.GlobalRev = rev
		}
		return nil
	case "TXN", "txn":
		raw := cmd.Key
		if raw == "" {
			raw = cmd.Value
		}
		txn := NewTransaction(raw)
		res := kv.Engine.Txn(&txn)
		if res != nil && res.Success && res.Rev > 0 {
			kv.GlobalRev = res.Rev
		}
		return res
	default:
		rev, rec := kv.Engine.Put(cmd.Key, []byte(cmd.Value))
		kv.GlobalRev = rev
		return string(rec.Value)
	}
}
