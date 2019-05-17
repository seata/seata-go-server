package storage

import (
	"net/url"

	"github.com/fagongzi/log"
)

const (
	protocolCell  = "cell"
	protocolRedis = "redis"
)

const (
	paramMaxRetryTimes = "retry"
)

// CreateStorage returns
func CreateStorage(protocolAddr string) (Storage, error) {
	u, err := url.Parse(protocolAddr)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case protocolCell:
		return createRedisLikeStorage(u, true)
	case protocolRedis:
		return createRedisLikeStorage(u, false)
	}

	log.Fatalf("the schema %s is not support", u.Scheme)
	return nil, nil
}
