package mem

import (
	"sync"

	"github.com/google/btree"
	"seata.io/server/pkg/meta"
)

type treeItem struct {
	key   uint64
	value *meta.GlobalTransaction
}

// Less returns true if the item key is less than the other.
func (item *treeItem) Less(other btree.Item) bool {
	left := item.key
	right := other.(*treeItem).key
	return right > left
}

// Equals returns true if the item key is equals the other.
func (item *treeItem) Equals(other btree.Item) bool {
	left := item.key
	right := other.(*treeItem).key
	return right == left
}

// kvTree kv btree
type kvTree struct {
	sync.RWMutex
	tree *btree.BTree
}

// newKVTree return a kv btree
func newKVTree() *kvTree {
	return &kvTree{
		tree: btree.New(64),
	}
}

// Count returns number of currently values
func (kv *kvTree) Count() int {
	return kv.tree.Len()
}

// Put puts a key, value to the tree
func (kv *kvTree) Put(key uint64, value *meta.GlobalTransaction) {
	kv.Lock()

	kv.tree.ReplaceOrInsert(&treeItem{
		key:   key,
		value: value,
	})

	kv.Unlock()
}

// Delete deletes a key, return false if not the key is not exists
func (kv *kvTree) Delete(key uint64) bool {
	kv.Lock()
	defer kv.Unlock()

	item := &treeItem{key: key}
	return nil != kv.tree.Delete(item)
}

// RangeDelete deletes key in [start, end)
func (kv *kvTree) RangeDelete(start, end uint64) {
	kv.Lock()
	defer kv.Unlock()

	var items []btree.Item
	item := &treeItem{key: start}
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		target := i.(*treeItem)
		if target.key < end {
			items = append(items, i)
			return true
		}

		return false
	})

	for _, target := range items {
		kv.tree.Delete(target)
	}
}

// Get get value, return nil if not the key is not exists
func (kv *kvTree) Get(key uint64) *meta.GlobalTransaction {
	kv.RLock()
	defer kv.RUnlock()

	item := &treeItem{key: key}

	var result *treeItem
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*treeItem)
		return false
	})

	if result == nil || !result.Equals(item) {
		return nil
	}

	return result.value
}

// Seek returns the next key and value which key >= spec key
func (kv *kvTree) Seek(key uint64) (uint64, *meta.GlobalTransaction) {
	kv.RLock()
	defer kv.RUnlock()

	item := &treeItem{key: key}

	var result *treeItem
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*treeItem)
		return false
	})

	if result == nil {
		return 0, nil
	}

	return result.key, result.value
}

// Scan scans in [start, end]
func (kv *kvTree) Scan(start, end uint64, handler func(key uint64, value *meta.GlobalTransaction) (bool, error)) error {
	kv.RLock()
	var items []*treeItem
	item := &treeItem{key: start}
	kv.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		target := i.(*treeItem)
		if target.key <= end {
			items = append(items, target)
			return true
		}

		return false
	})
	kv.RUnlock()

	for _, target := range items {
		c, err := handler(target.key, target.value)
		if err != nil || !c {
			return err
		}
	}

	return nil
}
