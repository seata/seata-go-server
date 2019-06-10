package cell

import (
	"testing"

	"seata.io/server/pkg/cedis"
	"seata.io/server/pkg/meta"
)

func TestLockAndUnlockFromRedis(t *testing.T) {
	s := NewStorage(cedis.NewCedis(cedis.WithCellProxies("127.0.0.1:6379")))
	conn := s.cell.Get()
	_, err := conn.Do("FLUSHALL")
	if err != nil {
		t.Errorf("flushall failed with %+v", err)
		return
	}

	res := "res1"
	for i := 0; i < 2; i++ {
		ok, conflict, err := s.Lock(res, 1, meta.LockKey{
			Namespace: "t1",
			Key:       "1",
		}, meta.LockKey{
			Namespace: "t1",
			Key:       "2",
		})
		if err != nil {
			t.Errorf("lock failed with %+v", err)
			return
		}

		if !ok {
			t.Errorf("lock expect true but false")
			return
		}

		if conflict != "" {
			t.Errorf("conflict expect empty but %s", conflict)
			return
		}
	}

	ok, _, err := s.Lock(res, 2, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})
	if err != nil {
		t.Errorf("lock failed with %+v", err)
		return
	}

	if ok {
		t.Errorf("lock expect false but true")
		return
	}

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
	if err != nil {
		t.Errorf("lock after unlock failed with %+v", err)
		return
	}

	if !ok {
		t.Errorf("lock after unlock expect true but false")
		return
	}
}

func TestLockableFromRedis(t *testing.T) {
	s := NewStorage(cedis.NewCedis(cedis.WithCellProxies("127.0.0.1:6379")))
	conn := s.cell.Get()
	_, err := conn.Do("FLUSHALL")
	if err != nil {
		t.Errorf("flushall failed with %+v", err)
		return
	}

	res := "res1"
	ok, conflict, err := s.Lockable(res, 1, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	}, meta.LockKey{
		Namespace: "t1",
		Key:       "2",
	})
	if err != nil {
		t.Errorf("lockable failed with %+v", err)
		return
	}

	if !ok {
		t.Errorf("lockable expect true but false")
		return
	}

	if conflict != "" {
		t.Errorf("conflict expect empty but %s", conflict)
		return
	}

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
	if err != nil {
		t.Errorf("lockable failed with %+v", err)
		return
	}

	if ok {
		t.Errorf("lockable expect false but true")
		return
	}
}
