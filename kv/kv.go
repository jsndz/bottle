package kv

type Command struct {
	Key   string
	Value string
	Op    string
}

type KVStore struct {
	commands map[string]Command
}

func NewKVStore() *KVStore {
	return &KVStore{
		commands: make(map[string]Command),
	}
}

func (kv *KVStore) Add(cmd Command, id string) {
	kv.commands[id] = cmd
}

func (kv *KVStore) Delete(id string) {
	delete(kv.commands, id)
}
