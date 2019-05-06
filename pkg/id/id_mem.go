package id

import (
	"sync/atomic"
)

type memGenerator struct {
	value uint64
}

func (g *memGenerator) Gen() (uint64, error) {
	return atomic.AddUint64(&g.value, 1), nil
}
