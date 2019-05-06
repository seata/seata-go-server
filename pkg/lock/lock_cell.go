package lock

import (
	"github.com/garyburd/redigo/redis"
	"github.com/infinivision/taas/pkg/cedis"
	"github.com/infinivision/taas/pkg/meta"
)

// NewCellResourceLocker returns a lock base on cell
func NewCellResourceLocker(cell *cedis.Cedis) ResourceLock {
	return &resourceLocker{
		cell: cell,
	}
}

type resourceLocker struct {
	cell *cedis.Cedis
}

// Lockable returns the key is lockable on resource
func (l *resourceLocker) Lockable(resource string, gid uint64, keys ...meta.LockKey) (bool, string, error) {
	if len(keys) == 0 {
		return true, "", nil
	}

	conn := l.cell.Get()
	args := make([]interface{}, 0, len(keys)+2)
	args = append(args, resource)
	args = append(args, gid)
	for _, key := range keys {
		args = append(args, key.Value())
	}

	rsp, err := redis.String(conn.Do("LOCKABLE", args...))
	conn.Close()

	if err != nil {
		return false, "", err
	}

	return rsp == "OK", rsp, nil
}

// Lock get the lock on the resource
func (l *resourceLocker) Lock(resource string, gid uint64, keys ...meta.LockKey) (bool, string, error) {
	if len(keys) == 0 {
		return true, "", nil
	}

	conn := l.cell.Get()
	args := make([]interface{}, 0, len(keys)+2)
	args = append(args, resource)
	args = append(args, gid)
	for _, key := range keys {
		args = append(args, key.Value())
	}

	rsp, err := redis.String(conn.Do("LOCK", args...))
	conn.Close()

	if err != nil {
		return false, "", err
	}

	return rsp == "OK", rsp, nil
}

// Unlock release the lock on the resource
func (l *resourceLocker) Unlock(resource string, keys ...meta.LockKey) error {
	if len(keys) == 0 {
		return nil
	}

	conn := l.cell.Get()
	args := make([]interface{}, 0, len(keys)+1)
	args = append(args, resource)
	for _, key := range keys {
		args = append(args, key.Value())
	}

	_, err := conn.Do("UNLOCK", args...)
	conn.Close()
	return err
}
