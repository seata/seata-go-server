package prophet

import (
	"errors"
)

var (
	// ErrSchedulerExisted error with scheduler is existed
	ErrSchedulerExisted = errors.New("scheduler is existed")
	// ErrSchedulerNotFound error with scheduler is not found
	ErrSchedulerNotFound = errors.New("scheduler is not found")
)
