package util

import (
	"time"

	"github.com/fagongzi/goetty"
)

var (
	// DefaultTW default TW
	DefaultTW = goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Millisecond * 100))
)
