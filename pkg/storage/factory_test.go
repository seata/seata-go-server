package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisCreateStorage(t *testing.T) {
	_, err := CreateStorage("redis://127.0.0.1:6379?retry=3&maxActive=100&maxIdle=10&idleTimeout=30&dailTimeout=10&readTimeout=30&writeTimeout=10")
	assert.Nil(t, err, "create redis storage url failed")
}

func TestCellCreateStorage(t *testing.T) {
	_, err := CreateStorage("cell://127.0.0.1:6379?proxy=127.0.0.1:6380&retry=3&maxActive=100&maxIdle=10&idleTimeout=30&dailTimeout=10&readTimeout=30&writeTimeout=10")
	assert.Nil(t, err, "create cell storage url failed")
}
