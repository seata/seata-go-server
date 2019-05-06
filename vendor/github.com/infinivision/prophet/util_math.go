package prophet

import (
	"math"
	"time"
)

var (
	bootstrapBalanceCount = uint64(10)
	bootstrapBalanceDiff  = float64(2)
)

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// minBalanceDiff returns the minimal diff to do balance. The formula is based
// on experience to let the diff increase alone with the count slowly.
func minBalanceDiff(count uint64) float64 {
	if count < bootstrapBalanceCount {
		return bootstrapBalanceDiff
	}
	return math.Sqrt(float64(count))
}

// getStringValue get string value
// if value if "" reutrn default value
func getStringValue(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}

	return value
}
