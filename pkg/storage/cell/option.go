package cell

// Option option funcation
type Option func(*options)

type options struct {
	maxRetryTimes int
	isCell        bool
}

// WithRetry set max retry times
func WithRetry(value int) Option {
	return func(opts *options) {
		opts.maxRetryTimes = value
	}
}

// WithElasticell set is the storage is elasticell or redis
// The elasticell expanded some commands like 'lock', 'range load hash'
func WithElasticell(value bool) Option {
	return func(opts *options) {
		opts.isCell = value
	}
}
