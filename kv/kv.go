package kv

import "encoding/json"

type Command struct {
	Key   string
	Value string
	Op    string
}

type KVStore struct {
	commands []Command
}

func NewKVStore() *KVStore {
	return &KVStore{
		commands: make([]Command, 0),
	}
}

func (kv *KVStore) Apply(data []byte) any {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return err
	}
	kv.commands = append(kv.commands, cmd)
	return nil
}
