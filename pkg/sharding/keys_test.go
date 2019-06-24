package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFragmentKey(t *testing.T) {
	data := getFragmentKey(1)
	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, data, "check fragment key")
}
