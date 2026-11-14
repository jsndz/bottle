package kv

import "fmt"

type Record struct {
	Key         string
	Value       []byte
	CreatedRev  int64
	ModifiedRev int64
	Version     int64
	IsDeleted   bool
	PrevRev     int64
}

type Storage struct {
	tree *BTree
}

func NewStorage() *Storage {
	return &Storage{
		tree: NewBTree(32),
	}
}

func formatRevKey(rev int64, key string) string {
	return fmt.Sprintf("%020d/%s", rev, key)
}

func (s *Storage) Put(rev int64, key string, rec *Record) {
	s.tree.Put(formatRevKey(rev, key), rec)
}

func (s *Storage) Get(rev int64, key string) (*Record, bool) {
	val, ok := s.tree.Get(formatRevKey(rev, key))
	if !ok {
		return nil, false
	}
	return val.(*Record), true
}

func (s *Storage) Range(startRev, endRev int64) []*Record {
	var records []*Record
	startKey := fmt.Sprintf("%020d/", startRev)
	var endKey string
	if endRev > 0 {
		endKey = fmt.Sprintf("%020d/", endRev+1)
	}
	s.tree.AscendRange(startKey, endKey, func(k string, v any) bool {
		records = append(records, v.(*Record))
		return true
	})
	return records
}
