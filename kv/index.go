package kv

type Index struct {
	tree *BTree
}

type Data struct {
	Key string
	Rev int64
}

func NewIndex() *Index {
	return &Index{
		tree: NewBTree(32),
	}
}

func (idx *Index) Get(key string) (int64, bool) {
	val, ok := idx.tree.Get(key)
	if !ok {
		return 0, false
	}
	return val.(int64), true
}

func (idx *Index) GET(key string) (int64, bool) {
	return idx.Get(key)
}

func (idx *Index) Put(key string, rev int64) {
	idx.tree.Put(key, rev)
}

func (idx *Index) Delete(key string) {
	idx.tree.Delete(key)
}

func (idx *Index) Prefix(prefix string) []Data {
	var results []Data

	idx.tree.AscendPrefix(prefix, func(key string, val any) bool {
		results = append(results, Data{Key: key, Rev: val.(int64)})
		return true
	})
	return results
}
