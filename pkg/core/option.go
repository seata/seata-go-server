package core

import (
	"time"

	"github.com/infinivision/taas/pkg/election"
	"github.com/infinivision/taas/pkg/id"
	"github.com/infinivision/taas/pkg/storage"
)

// Option option
type Option func(*options)

type options struct {
	gen                                id.Generator
	storage                            storage.Storage
	retries                            int
	transactionTimeout                 time.Duration
	ackTimeout                         time.Duration
	commitIfAllBranchSucceedInPhaseOne bool
	elector                            election.Elector
	electorOptions                     []election.Option
	concurrency                        int
	becomeLeader, becomeFollower       func()
}

func (opts *options) adjust() {
	if opts.gen == nil {
		opts.gen = id.NewMemGenerator()
	}

	if opts.transactionTimeout == 0 {
		opts.transactionTimeout = time.Second * 5
	}

	if opts.ackTimeout == 0 {
		opts.ackTimeout = time.Second * 5
	}

	if opts.retries == 0 {
		opts.retries = 3
	}

	if opts.concurrency == 0 {
		opts.concurrency = 1024
	}
}

// WithStorage set meta storage options
func WithStorage(value storage.Storage) Option {
	return func(opts *options) {
		opts.storage = value
	}
}

// WithIDGenerator set id generator
func WithIDGenerator(value id.Generator) Option {
	return func(opts *options) {
		opts.gen = value
	}
}

// WithElectorOptions set leader elector options
func WithElectorOptions(value ...election.Option) Option {
	return func(opts *options) {
		opts.electorOptions = append(opts.electorOptions, value...)
	}
}

// WithElector set elector
func WithElector(value election.Elector) Option {
	return func(opts *options) {
		opts.elector = value
	}
}

// WithTransactionTimeout set transaction timeout
func WithTransactionTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.transactionTimeout = value
	}
}

// WithACKTimeout set ackTimeout times
func WithACKTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.ackTimeout = value
	}
}

// WithCommitIfAllBranchSucceedInPhaseOne set commitIfAllBranchSucceedInPhaseOne
func WithCommitIfAllBranchSucceedInPhaseOne(value bool) Option {
	return func(opts *options) {
		opts.commitIfAllBranchSucceedInPhaseOne = value
	}
}

// WithStatusChangeAware set leader follow handler
func WithStatusChangeAware(becomeLeader, becomeFollower func()) Option {
	return func(opts *options) {
		opts.becomeLeader = becomeLeader
		opts.becomeFollower = becomeFollower
	}
}

// WithConcurrency set concurrcy
func WithConcurrency(concurrency int) Option {
	return func(opts *options) {
		opts.concurrency = concurrency
	}
}
