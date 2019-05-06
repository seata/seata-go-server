package util

import (
	"github.com/shirou/gopsutil/mem"
)

// MemStats returns the mem usage stats
func MemStats() (*mem.VirtualMemoryStat, error) {
	return mem.VirtualMemory()
}
