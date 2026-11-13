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
	data      map[string]string
	GlobalRev int64
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}

func (kv *KVStore) Apply(data []byte) any {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.GlobalRev++
	switch cmd.Op {
	case "PUT", "put", "SET", "set":
		kv.data[cmd.Key] = cmd.Value

		return cmd.Value
	case "DELETE", "delete", "DEL", "del":
		delete(kv.data, cmd.Key)
		return nil
	default:
		kv.data[cmd.Key] = cmd.Value
		return cmd.Value
	}
}
