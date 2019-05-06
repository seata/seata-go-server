package prophet

import (
	"fmt"
	"time"
)

// aggregationOperator is the aggregations all operator on the same resource
type aggregationOperator struct {
	cfg       *Cfg
	ID        uint64     `json:"id"`
	StartAt   time.Time  `json:"startAt"`
	EndAt     time.Time  `json:"endAt"`
	LastIndex int        `json:"lastIndex"`
	Ops       []Operator `json:"ops"`
}

func newAggregationOp(cfg *Cfg, target *ResourceRuntime, ops ...Operator) Operator {
	if len(ops) == 0 {
		log.Fatal("prophet: create new resource aggregation operator use empty opts")
	}

	return &aggregationOperator{
		cfg:     cfg,
		ID:      target.meta.ID(),
		StartAt: time.Now(),
		Ops:     ops,
	}
}

func (op *aggregationOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *aggregationOperator) ResourceID() uint64 {
	return op.ID
}

func (op *aggregationOperator) ResourceKind() ResourceKind {
	return op.Ops[0].ResourceKind()
}

func (op *aggregationOperator) Do(target *ResourceRuntime) (*resourceHeartbeatRsp, bool) {
	if time.Since(op.StartAt) > op.cfg.TimeoutWaitOperatorComplete {
		log.Errorf("prophet: operator %s timeout", op)
		return nil, true
	}

	// If an operator is not finished, do it.
	for ; op.LastIndex < len(op.Ops); op.LastIndex++ {
		if res, finished := op.Ops[op.LastIndex].Do(target); !finished {
			return res, false
		}
	}

	op.EndAt = time.Now()
	return nil, true
}
