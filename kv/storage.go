package kv

type Record struct {
	Key         string
	Value       []byte
	CreatedRev  int64
	ModifiedRev int64
	Version     int64
	IsDeleted   bool
	PrevRev     int64
}
