package kv

import (
	"encoding/json"
	"sync"

	"github.com/jsndz/bottle/watch"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type EventCallback func(e watch.Event)

type KVStore struct {
	mu        sync.RWMutex
	Engine    *MVCCEngine
	GlobalRev int64
	onEvent   EventCallback
}

func NewKVStore() *KVStore {
	return &KVStore{
		Engine:    NewMVCCEngine(),
		GlobalRev: 0,
	}
}

func (kv *KVStore) SetEventCallback(e EventCallback) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.onEvent = e
}

func (kv *KVStore) Get(key string) (*Record, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	rec, ok := kv.Engine.Get(key)
	if !ok {
		return nil, false
	}
	return rec, true
}

func (kv *KVStore) GetPrefix(prefix string) []*Record {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.Engine.GetPrefix(prefix)
}

func (kv *KVStore) Revision() int64 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.GlobalRev
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
		val := []byte(cmd.Value)
		var prevVal []byte
		if prevRec, ok := kv.Engine.Get(cmd.Key); ok {
			prevVal = prevRec.Value
		}
		rev, rec := kv.Engine.Put(cmd.Key, val)
		kv.GlobalRev = rev
		if kv.onEvent != nil {
			event := watch.NewEvent(watch.PUT, cmd.Key, val, prevVal, rev)
			kv.onEvent(event)
		}
		return string(rec.Value)
	case "DELETE", "delete", "DEL", "del":
		rev, prev, ok := kv.Engine.Delete(cmd.Key)
		if ok {
			kv.GlobalRev = rev
			if kv.onEvent != nil {
				var prevVal []byte
				if prev != nil {
					prevVal = prev.Value
				}
				event := watch.NewEvent(watch.DELETE, cmd.Key, nil, prevVal, rev)
				kv.onEvent(event)
			}
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
		if kv.onEvent != nil && res != nil {
			for _, ev := range res.Events {
				kv.onEvent(ev)
			}
		}
		return res
	default:
		val := []byte(cmd.Value)
		var prevVal []byte
		if prevRec, ok := kv.Engine.Get(cmd.Key); ok {
			prevVal = prevRec.Value
		}
		rev, rec := kv.Engine.Put(cmd.Key, val)
		kv.GlobalRev = rev
		if kv.onEvent != nil {
			event := watch.NewEvent(watch.PUT, cmd.Key, val, prevVal, rev)
			kv.onEvent(event)
		}
		return string(rec.Value)
	}
}
