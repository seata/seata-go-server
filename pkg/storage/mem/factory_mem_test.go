package mem

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

func TestCountAndPut(t *testing.T) {
	s := NewStorage()
	c, err := s.Count(1)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, uint64(0), c, "check mem storage failed")

	g := &meta.GlobalTransaction{ID: 1}
	err = s.Put(1, g)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)

	c, err = s.Count(1)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, uint64(1), c, "check mem storage failed")
}

func TestGetAndPut(t *testing.T) {
	s := NewStorage()
	g, err := s.Get(1, 1)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Nil(t, g, "check mem storage failed")

	g = &meta.GlobalTransaction{ID: 1}
	err = s.Put(1, g)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)

	g, err = s.Get(1, 1)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, uint64(1), g.ID, "check mem storage failed")
}

func TestRemoveAndPut(t *testing.T) {
	s := NewStorage()

	g := &meta.GlobalTransaction{ID: 1}
	err := s.Put(1, g)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)

	g, err = s.Get(1, 1)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, uint64(1), g.ID, "check mem storage failed")

	err = s.Remove(1, g)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)

	g, err = s.Get(1, 1)
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Nil(t, g, "check mem storage failed")
}

func TestLoad(t *testing.T) {
	s := NewStorage()

	for i := uint64(0); i < 128; i++ {
		s.Put(1, &meta.GlobalTransaction{ID: i})
	}

	q := meta.EmptyQuery
	q.Limit = 128
	c := 0
	err := s.Load(1, q, func(g *meta.GlobalTransaction) error {
		c++
		return nil
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, 128, c, "check mem storage failed")

	q.Limit = 127
	c = 0
	err = s.Load(1, q, func(g *meta.GlobalTransaction) error {
		c++
		return nil
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, 127, c, "check mem storage failed")
}

func TestPutManual(t *testing.T) {
	s := NewStorage()

	err := s.PutManual(1, &meta.Manual{})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, 1, len(s.manuals[1]), "check mem storage failed")

	err = s.PutManual(1, &meta.Manual{})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, 2, len(s.manuals[1]), "check mem storage failed")
}

func TestManual(t *testing.T) {
	s := NewStorage()

	c, err := s.Manual(1, func(m *meta.Manual) error {
		return nil
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, 0, c, "check mem storage failed")

	s.PutManual(1, &meta.Manual{})
	s.PutManual(1, &meta.Manual{})

	c, err = s.Manual(1, func(m *meta.Manual) error {
		return nil
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.Equal(t, 2, c, "check mem storage failed")
	assert.Equal(t, 0, len(s.manuals[1]), "check mem storage failed")
}

func TestLockAndUnlock(t *testing.T) {
	s := NewStorage()

	res := "res1"
	for i := 0; i < 2; i++ {
		ok, conflict, err := s.Lock(res, 1, meta.LockKey{
			Namespace: "t1",
			Key:       "1",
		}, meta.LockKey{
			Namespace: "t1",
			Key:       "2",
		})
		assert.Nilf(t, err, "check mem storage failed with %+v", err)
		assert.True(t, ok, "check mem storage failed")
		assert.Equal(t, "", conflict, "check mem storage failed")
	}

	ok, _, err := s.Lock(res, 2, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.True(t, !ok, "check mem storage failed")

	s.Unlock(res, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})

	ok, _, err = s.Lock(res, 2, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.True(t, ok, "check mem storage failed")
}

func TestLockable(t *testing.T) {
	s := NewStorage()

	res := "res1"
	ok, conflict, err := s.Lockable(res, 1, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.True(t, ok, "check mem storage failed")
	assert.Equal(t, "", conflict, "check mem storage failed")

	s.Lock(res, 2, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})

	ok, _, err = s.Lockable(res, 1, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})
	assert.Nilf(t, err, "check mem storage failed with %+v", err)
	assert.True(t, !ok, "check mem storage failed")
}
