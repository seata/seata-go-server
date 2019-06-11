package cell

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGKey(t *testing.T) {
	gid := uint64(1)
	key := gCellKey(gid)
	assert.Equal(t, "__seata_1_gid__", key, "gCellKey failed")
}

func TestManualKey(t *testing.T) {
	fid := uint64(1)
	key := manualCellKey(fid)
	assert.Equal(t, "__seata_1_manual__", key, "manualCellKey failed")
}
