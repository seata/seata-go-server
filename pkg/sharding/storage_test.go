package sharding

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fagongzi/util/json"
	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/local"
	"seata.io/server/pkg/meta"
)

func create(t *testing.T) storage {
	localS, err := local.NewBadgerStorage(fmt.Sprintf("%s/seata-data-%d", os.TempDir(), time.Now().Nanosecond()))
	assert.Nilf(t, err, "check storage failed with %+v", err)

	return newStorage(localS)
}

func TestGetAndSet(t *testing.T) {
	s := create(t)
	value, err := s.get([]byte("test-key"))
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, 0, len(value), "check storage failed")

	err = s.set([]byte("test-key"), []byte("seata"))
	assert.Nilf(t, err, "check storage failed with %+v", err)

	value, err = s.get([]byte("test-key"))
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, "seata", string(value), "check storage failed")

	s.set([]byte("test-key"), []byte("seata2"))
	value, err = s.get([]byte("test-key"))
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, "seata2", string(value), "check storage failed")
}

func TestCountFragmentsAndCreate(t *testing.T) {
	s := create(t)
	c, err := s.countFragments()
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, 0, c, "check storage failed")

	err = s.putFragment(meta.Fragment{ID: 1})
	assert.Nilf(t, err, "check storage failed with %+v", err)

	c, err = s.countFragments()
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, 1, c, "check storage failed")

	err = s.putFragment(meta.Fragment{ID: 2})
	assert.Nilf(t, err, "check storage failed with %+v", err)

	c, err = s.countFragments()
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, 2, c, "check storage failed")
}

func TestLoadFragments(t *testing.T) {
	s := create(t)

	var frags []*meta.Fragment
	fn := func(value []byte) (uint64, error) {
		frag := &meta.Fragment{}
		json.MustUnmarshal(frag, value)
		frags = append(frags, frag)
		return frag.ID, nil
	}

	err := s.loadFragments(fn)
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, 0, len(frags), "check storage failed")

	for i := 0; i < 128; i++ {
		s.putFragment(meta.Fragment{ID: uint64(i)})
	}

	err = s.loadFragments(fn)
	assert.Nilf(t, err, "check storage failed with %+v", err)
	assert.Equal(t, 128, len(frags), "check storage failed")
}

func TestPutFragment(t *testing.T) {
	s := create(t)

	var frags []*meta.Fragment
	fn := func(value []byte) (uint64, error) {
		frag := &meta.Fragment{}
		json.MustUnmarshal(frag, value)
		frags = append(frags, frag)
		return frag.ID, nil
	}

	s.putFragment(meta.Fragment{ID: 1, Version: 1})
	s.putFragment(meta.Fragment{ID: 1, Version: 2})
	s.loadFragments(fn)
	assert.Equal(t, uint64(2), frags[0].Version, "check storage failed")
}
