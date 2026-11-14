package kv

import (
	"sync"
)

type MVCCEngine struct {
	CurrentRev int64
	Index      *Index
	Storage    *Storage
	mu         sync.RWMutex
}

func NewMVCCEngine() *MVCCEngine {
	return &MVCCEngine{
		CurrentRev: 0,
		Index:      NewIndex(),
		Storage:    NewStorage(),
	}
}

func (e *MVCCEngine) Get(key string) (*Record, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rev, exist := e.Index.Get(key)
	if !exist {
		return nil, false
	}

	record, exist := e.Storage.Get(rev, key)
	if !exist || record.IsDeleted {
		return nil, false
	}

	return record, true
}

func (e *MVCCEngine) GET(key string) (*Record, bool) {
	return e.Get(key)
}

func (e *MVCCEngine) getInternal(key string) *Record {
	rev, exist := e.Index.Get(key)
	if !exist {
		return nil
	}
	record, exist := e.Storage.Get(rev, key)
	if !exist || record.IsDeleted {
		return nil
	}
	return record
}

func (e *MVCCEngine) putInternal(key string, value []byte, rev int64) *Record {
	oldRev, exists := e.Index.Get(key)

	var createRev int64 = rev
	var version int64 = 1
	var prevRev int64 = 0

	if exists {
		if old, ok := e.Storage.Get(oldRev, key); ok {
			createRev = old.CreatedRev
			version = old.Version + 1
			prevRev = oldRev
		}
	}

	record := &Record{
		Key:         key,
		Value:       value,
		CreatedRev:  createRev,
		ModifiedRev: rev,
		Version:     version,
		PrevRev:     prevRev,
		IsDeleted:   false,
	}
	e.Storage.Put(rev, key, record)
	e.Index.Put(key, rev)

	return record
}

func (e *MVCCEngine) deleteInternal(key string, rev int64) (*Record, bool) {
	oldRev, exist := e.Index.Get(key)
	if !exist {
		return nil, false
	}

	var prevRecord *Record
	var createdRev int64 = rev
	var version int64 = 1

	if old, ok := e.Storage.Get(oldRev, key); ok {
		prevRecord = old
		createdRev = old.CreatedRev
		version = old.Version + 1
	}

	tombstone := &Record{
		Key:         key,
		Value:       nil,
		CreatedRev:  createdRev,
		ModifiedRev: rev,
		Version:     version,
		PrevRev:     oldRev,
		IsDeleted:   true,
	}

	e.Storage.Put(rev, key, tombstone)
	e.Index.Delete(key)

	return prevRecord, true
}

func (e *MVCCEngine) Put(key string, value []byte) (int64, *Record) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.CurrentRev++
	record := e.putInternal(key, value, e.CurrentRev)
	return e.CurrentRev, record
}

func (e *MVCCEngine) PUT(key string, value []byte) (int64, *Record) {
	return e.Put(key, value)
}

func (e *MVCCEngine) GetAtRevision(key string, targetRev int64) (*Record, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rev, exist := e.Index.Get(key)
	if !exist {
		return nil, false
	}

	for rev > 0 {
		rec, ok := e.Storage.Get(rev, key)
		if !ok {
			return nil, false
		}
		if rec.ModifiedRev <= targetRev {
			if rec.IsDeleted {
				return nil, false
			}
			return rec, true
		}
		rev = rec.PrevRev
	}

	return nil, false
}

func (e *MVCCEngine) GetPrefix(prefix string) []*Record {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var records []*Record
	data := e.Index.Prefix(prefix)
	for _, d := range data {
		if record, exist := e.Storage.Get(d.Rev, d.Key); exist {
			if !record.IsDeleted {
				records = append(records, record)
			}
		}
	}
	return records
}

func (e *MVCCEngine) Delete(key string) (int64, *Record, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exist := e.Index.Get(key); !exist {
		return 0, nil, false
	}

	e.CurrentRev++
	delRev := e.CurrentRev
	prevRecord, ok := e.deleteInternal(key, delRev)
	return delRev, prevRecord, ok
}

func (e *MVCCEngine) DELETE(key string) {
	e.Delete(key)
}
