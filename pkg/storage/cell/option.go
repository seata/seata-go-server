package cell

// Option option funcation
type Option func(*options)

type options struct {
	maxRetryTimes int
}

// WithRetry set max retry times
func WithRetry(value int) Option {
	return func(opts *options) {
		opts.maxRetryTimes = value
	}
}
