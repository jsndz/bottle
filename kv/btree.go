package kv

import (
	"sort"
	"strings"
	"sync"
)

const (
	defaultBTreeOrder = 32
	minBTreeOrder     = 4
)

type bNode struct {
	isLeaf bool
	keys   []string

	children []*bNode

	values []any
	next   *bNode
	prev   *bNode

	parent *bNode
}

type BTree struct {
	mu    sync.RWMutex
	root  *bNode
	order int
	count int
}

func NewBTree(order int) *BTree {
	if order < minBTreeOrder {
		order = defaultBTreeOrder
	}
	return &BTree{
		root:  &bNode{isLeaf: true},
		order: order,
	}
}

func (t *BTree) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count
}

func (t *BTree) Get(key string) (any, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leaf := t.findLeaf(key)
	if leaf == nil {
		return nil, false
	}

	idx := sort.SearchStrings(leaf.keys, key)
	if idx < len(leaf.keys) && leaf.keys[idx] == key {
		return leaf.values[idx], true
	}
	return nil, false
}

func (t *BTree) Put(key string, val any) {
	t.Insert(key, val)
}

func (t *BTree) Insert(key string, val any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	leaf := t.findLeaf(key)
	if leaf == nil {
		leaf = &bNode{isLeaf: true}
		t.root = leaf
	}

	idx := sort.SearchStrings(leaf.keys, key)
	if idx < len(leaf.keys) && leaf.keys[idx] == key {
		leaf.values[idx] = val
		return
	}

	leaf.keys = insertString(leaf.keys, idx, key)
	leaf.values = insertValue(leaf.values, idx, val)
	t.count++

	if len(leaf.keys) > t.order {
		t.splitLeaf(leaf)
	}
}

func (t *BTree) Delete(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	leaf := t.findLeaf(key)
	if leaf == nil {
		return false
	}

	idx := sort.SearchStrings(leaf.keys, key)
	if idx >= len(leaf.keys) || leaf.keys[idx] != key {
		return false
	}

	leaf.keys = append(leaf.keys[:idx], leaf.keys[idx+1:]...)
	leaf.values = append(leaf.values[:idx], leaf.values[idx+1:]...)
	t.count--

	if leaf == t.root {
		return true
	}

	if len(leaf.keys) == 0 {
		if leaf.prev != nil {
			leaf.prev.next = leaf.next
		}
		if leaf.next != nil {
			leaf.next.prev = leaf.prev
		}
		t.removeFromParent(leaf)
	}

	return true
}

func (t *BTree) Ascend(iterator func(key string, val any) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	curr := t.firstLeaf()
	for curr != nil {
		for i := 0; i < len(curr.keys); i++ {
			if !iterator(curr.keys[i], curr.values[i]) {
				return
			}
		}
		curr = curr.next
	}
}

func (t *BTree) AscendRange(startKey, endKey string, iterator func(key string, val any) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	curr := t.findLeaf(startKey)
	if curr == nil {
		return
	}

	startIdx := sort.SearchStrings(curr.keys, startKey)

	for curr != nil {
		for i := startIdx; i < len(curr.keys); i++ {
			if endKey != "" && curr.keys[i] >= endKey {
				return
			}
			if !iterator(curr.keys[i], curr.values[i]) {
				return
			}
		}
		startIdx = 0
		curr = curr.next
	}
}

func (t *BTree) AscendPrefix(prefix string, iterator func(key string, val any) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	curr := t.findLeaf(prefix)
	if curr == nil {
		return
	}

	startIdx := sort.SearchStrings(curr.keys, prefix)

	for curr != nil {
		for i := startIdx; i < len(curr.keys); i++ {
			if !strings.HasPrefix(curr.keys[i], prefix) {
				return
			}
			if !iterator(curr.keys[i], curr.values[i]) {
				return
			}
		}
		startIdx = 0
		curr = curr.next
	}
}

func (t *BTree) findLeaf(key string) *bNode {
	curr := t.root
	for curr != nil && !curr.isLeaf {
		idx := sort.Search(len(curr.keys), func(i int) bool {
			return key < curr.keys[i]
		})
		curr = curr.children[idx]
	}
	return curr
}

func (t *BTree) firstLeaf() *bNode {
	curr := t.root
	for curr != nil && !curr.isLeaf {
		if len(curr.children) == 0 {
			return nil
		}
		curr = curr.children[0]
	}
	return curr
}

func (t *BTree) splitLeaf(leaf *bNode) {
	mid := len(leaf.keys) / 2

	right := &bNode{
		isLeaf: true,
		parent: leaf.parent,
	}

	right.keys = append(right.keys, leaf.keys[mid:]...)
	right.values = append(right.values, leaf.values[mid:]...)

	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]

	right.next = leaf.next
	right.prev = leaf
	if leaf.next != nil {
		leaf.next.prev = right
	}
	leaf.next = right

	promotedKey := right.keys[0]

	if leaf == t.root {
		newRoot := &bNode{
			isLeaf:   false,
			keys:     []string{promotedKey},
			children: []*bNode{leaf, right},
		}
		leaf.parent = newRoot
		right.parent = newRoot
		t.root = newRoot
		return
	}

	t.insertIntoParent(leaf, promotedKey, right)
}

func (t *BTree) splitInternal(node *bNode) {
	mid := len(node.keys) / 2
	promotedKey := node.keys[mid]

	right := &bNode{
		isLeaf: false,
		parent: node.parent,
	}

	right.keys = append(right.keys, node.keys[mid+1:]...)
	right.children = append(right.children, node.children[mid+1:]...)

	for _, child := range right.children {
		child.parent = right
	}

	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]

	if node == t.root {
		newRoot := &bNode{
			isLeaf:   false,
			keys:     []string{promotedKey},
			children: []*bNode{node, right},
		}
		node.parent = newRoot
		right.parent = newRoot
		t.root = newRoot
		return
	}

	t.insertIntoParent(node, promotedKey, right)
}

func (t *BTree) insertIntoParent(left *bNode, key string, right *bNode) {
	parent := left.parent
	right.parent = parent

	idx := 0
	for idx < len(parent.children) && parent.children[idx] != left {
		idx++
	}

	parent.keys = insertString(parent.keys, idx, key)
	parent.children = insertChild(parent.children, idx+1, right)

	if len(parent.keys) > t.order {
		t.splitInternal(parent)
	}
}

func (t *BTree) removeFromParent(node *bNode) {
	if node == t.root {
		return
	}

	parent := node.parent
	idx := 0
	for idx < len(parent.children) && parent.children[idx] != node {
		idx++
	}

	if idx == len(parent.children) {
		return
	}

	if idx == 0 {
		parent.children = parent.children[1:]
		if len(parent.keys) > 0 {
			parent.keys = parent.keys[1:]
		}
	} else {
		parent.children = append(parent.children[:idx], parent.children[idx+1:]...)
		parent.keys = append(parent.keys[:idx-1], parent.keys[idx:]...)
	}

	if parent == t.root {
		if len(parent.keys) == 0 {
			if len(parent.children) > 0 {
				t.root = parent.children[0]
				t.root.parent = nil
			} else {
				t.root = &bNode{isLeaf: true}
			}
		}
		return
	}

	if len(parent.keys) == 0 {
		t.removeFromParent(parent)
	}
}

func insertString(slice []string, index int, value string) []string {
	slice = append(slice, "")
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}

func insertValue(slice []any, index int, value any) []any {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}

func insertChild(slice []*bNode, index int, value *bNode) []*bNode {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}
