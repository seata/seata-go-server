package cedis

import (
	"time"
)

// Option Elasticell redis client option
type Option func(*options)

type options struct {
	proxies      []string
	maxActive    int
	maxIdle      int
	idleTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	dailTimeout  time.Duration
}

func (opts *options) adjust() {
	if len(opts.proxies) == 0 {
		opts.proxies = []string{"localhost:6379"}
	}

	if opts.maxActive == 0 {
		opts.maxActive = 10
	}

	if opts.maxIdle == 0 {
		opts.maxIdle = 1
	}

	if opts.idleTimeout == 0 {
		opts.idleTimeout = time.Second * 30
	}

	if opts.readTimeout == 0 {
		opts.readTimeout = time.Second * 5
	}

	if opts.writeTimeout == 0 {
		opts.writeTimeout = time.Second * 5
	}

	if opts.dailTimeout == 0 {
		opts.dailTimeout = time.Second * 10
	}
}

// WithCellProxies set elasticell proxy address
func WithCellProxies(value ...string) Option {
	return func(opts *options) {
		opts.proxies = value
	}
}

// WithMaxActive set max active conn per elasticell proxy
func WithMaxActive(value int) Option {
	return func(opts *options) {
		opts.maxActive = value
	}
}

// WithMaxIdle set max idle conn per elasticell proxy
func WithMaxIdle(value int) Option {
	return func(opts *options) {
		opts.maxIdle = value
	}
}

// WithIdleTimeout set max idle conn per elasticell proxy
func WithIdleTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.idleTimeout = value
	}
}

// WithReadTimeout set read timeout for connection
func WithReadTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.readTimeout = value
	}
}

// WithWriteTimeout set write timeout for connection
func WithWriteTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.writeTimeout = value
	}
}

// WithDailTimeout set dail timeout for connection
func WithDailTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.dailTimeout = value
	}
}
