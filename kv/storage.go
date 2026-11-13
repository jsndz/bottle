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

func formatRev(rev int64) string {
	return fmt.Sprintf("%020d", rev)
}

func (s *Storage) Put(rev int64, rec *Record) {
	s.tree.Put(formatRev(rev), rec)
}

func (s *Storage) Get(rev int64) (*Record, bool) {
	val, ok := s.tree.Get(formatRev(rev))
	if !ok {
		return nil, false
	}
	return val.(*Record), true
}

func (s *Storage) Range(startRev, endRev int64) []*Record {
	var records []*Record
	var endKey string
	if endRev > 0 {
		endKey = formatRev(endRev + 1)
	}
	s.tree.AscendRange(formatRev(startRev), endKey, func(k string, v any) bool {
		records = append(records, v.(*Record))
		return true
	})
	return records
}
