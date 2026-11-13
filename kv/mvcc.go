package kv

import "sync"

type MVCCEngine struct {
	CurrentRev int64
	index      *BTree
	Storage    *BTree
	mu         sync.Mutex
}

func NewMVCCEngine() *MVCCEngine {
	return &MVCCEngine{
		CurrentRev: 0,
		index:      NewBTree(32),
		Storage:    NewBTree(32),
	}
}

func (e *MVCCEngine) GET(key string) {}

func (e *MVCCEngine) PUT(key string, value []byte) {}

func (e *MVCCEngine) GetAtRevision(key string, targetRev int64)

func (e *MVCCEngine) GetPrefix(prefix string)

func (e *MVCCEngine) DELETE(key string)
