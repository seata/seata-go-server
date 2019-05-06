package cedis

import (
	"sync/atomic"

	"github.com/garyburd/redigo/redis"
)

// Cedis Elasticell redis client
type Cedis struct {
	ops        uint64
	opts       options
	proxyPools []*redis.Pool
}

// NewCedis create a elasticell client
func NewCedis(opts ...Option) *Cedis {
	c := &Cedis{}
	for _, opt := range opts {
		opt(&c.opts)
	}
	c.opts.adjust()

	for _, proxy := range c.opts.proxies {
		c.proxyPools = append(c.proxyPools, &redis.Pool{
			MaxActive:   c.opts.maxActive,
			MaxIdle:     c.opts.maxIdle,
			IdleTimeout: c.opts.idleTimeout,
			Wait:        true,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp",
					proxy,
					redis.DialWriteTimeout(c.opts.writeTimeout),
					redis.DialConnectTimeout(c.opts.dailTimeout),
					redis.DialReadTimeout(c.opts.readTimeout))
			},
		})
	}

	return c
}

// Get returns a redis connection
func (c *Cedis) Get() redis.Conn {
	return c.proxyPools[int(atomic.AddUint64(&c.ops, 1)%uint64(len(c.proxyPools)))].Get()
}
