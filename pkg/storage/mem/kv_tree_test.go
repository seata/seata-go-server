package mem

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

func TestKVTreePutAndGet(t *testing.T) {
	tree := newKVTree()

	v := tree.Get(1)
	assert.Nil(t, v, "check kv tree failed")

	value := &meta.GlobalTransaction{ID: 1}
	tree.Put(value.ID, value)

	v = tree.Get(value.ID)
	assert.NotNil(t, v, "check kv tree failed")
}

func TestKVTreeDelete(t *testing.T) {
	tree := newKVTree()

	value := &meta.GlobalTransaction{ID: 1}
	assert.True(t, !tree.Delete(value.ID), "check kv tree failed")

	tree.Put(value.ID, value)
	assert.True(t, tree.Delete(value.ID), "check kv tree failed")

	v := tree.Get(value.ID)
	assert.Nil(t, v, "check kv tree failed")
}

func TestKVTreeRangeDelete(t *testing.T) {
	tree := newKVTree()

	value1 := &meta.GlobalTransaction{ID: 1}
	value2 := &meta.GlobalTransaction{ID: 2}
	value3 := &meta.GlobalTransaction{ID: 3}
	value4 := &meta.GlobalTransaction{ID: 4}

	tree.Put(value1.ID, value1)
	tree.Put(value2.ID, value2)
	tree.Put(value3.ID, value3)
	tree.Put(value4.ID, value4)

	tree.RangeDelete(value1.ID, value4.ID)

	v := tree.Get(value1.ID)
	assert.Nil(t, v, "check kv tree failed")

	v = tree.Get(value2.ID)
	assert.Nil(t, v, "check kv tree failed")

	v = tree.Get(value3.ID)
	assert.Nil(t, v, "check kv tree failed")

	v = tree.Get(value4.ID)
	assert.NotNil(t, v, "check kv tree failed")
}

func TestKVTreeSeek(t *testing.T) {
	tree := newKVTree()

	value1 := &meta.GlobalTransaction{ID: 1}
	value2 := &meta.GlobalTransaction{ID: 2}
	value3 := &meta.GlobalTransaction{ID: 3}
	value4 := &meta.GlobalTransaction{ID: 4}

	tree.Put(value1.ID, value1)
	tree.Put(value3.ID, value3)

	_, v := tree.Seek(value1.ID)
	assert.Equal(t, value1.ID, v.ID, "check kv tree failed")

	_, v = tree.Seek(value2.ID)
	assert.Equal(t, value3.ID, v.ID, "check kv tree failed")

	_, v = tree.Seek(value3.ID)
	assert.Equal(t, value3.ID, v.ID, "check kv tree failed")

	_, v = tree.Seek(value4.ID)
	assert.Nil(t, v, "check kv tree failed")
}

func TestKVTreeScan(t *testing.T) {
	tree := newKVTree()

	value1 := &meta.GlobalTransaction{ID: 1}
	value2 := &meta.GlobalTransaction{ID: 2}
	value3 := &meta.GlobalTransaction{ID: 3}
	value4 := &meta.GlobalTransaction{ID: 4}

	tree.Put(value1.ID, value1)
	tree.Put(value2.ID, value2)
	tree.Put(value3.ID, value3)
	tree.Put(value4.ID, value4)

	cnt := 0
	err := tree.Scan(value1.ID, value4.ID, func(key uint64, value *meta.GlobalTransaction) (bool, error) {
		cnt++
		return true, nil
	})
	assert.Equal(t, 4, cnt, "check kv tree failed")

	cnt = 0
	err = tree.Scan(value1.ID, value4.ID, func(key uint64, value *meta.GlobalTransaction) (bool, error) {
		if key == value2.ID {
			return false, nil
		}

		cnt++
		return true, nil
	})
	assert.Equal(t, 1, cnt, "check kv tree failed")

	cnt = 0
	err = tree.Scan(value1.ID, value4.ID, func(key uint64, value *meta.GlobalTransaction) (bool, error) {
		if key == value2.ID {
			return true, fmt.Errorf("err")
		}

		cnt++
		return true, nil
	})
	assert.NotNil(t, err, "check kv tree failed")
	assert.Equal(t, 1, cnt, "check kv tree failed")

	err = tree.Scan(value1.ID, value4.ID, func(key uint64, value *meta.GlobalTransaction) (bool, error) {
		tree.Delete(key)
		return true, nil
	})
	assert.Nil(t, err, "check kv tree failed")

	v := tree.Get(value1.ID)
	assert.Nil(t, v, "check kv tree failed")

	v = tree.Get(value2.ID)
	assert.Nil(t, v, "check kv tree failed")

	v = tree.Get(value3.ID)
	assert.Nil(t, v, "check kv tree failed")

	v = tree.Get(value4.ID)
	assert.Nil(t, v, "check kv tree failed")
}
