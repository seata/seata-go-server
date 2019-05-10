package core

import (
	"time"
)

func millisecond(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
