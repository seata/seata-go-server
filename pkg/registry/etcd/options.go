package etcd

// Option option
type Option func(*options)

type options struct {
	leaseTTL int64
	group    string
}

func (opts *options) adjust() {
	if opts.leaseTTL == 0 {
		opts.leaseTTL = 10
	}

	if opts.group == "" {
		opts.group = "default"
	}
}

// WithLease set lease ttl, unit is seconds
func WithLease(value int64) Option {
	return func(opts *options) {
		opts.leaseTTL = value
	}
}

// WithGroup set group
func WithGroup(value string) Option {
	return func(opts *options) {
		opts.group = value
	}
}
