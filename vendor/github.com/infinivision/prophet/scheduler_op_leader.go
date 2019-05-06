package prophet

import (
	"fmt"
)

func newTransferLeaderAggregationOp(cfg *Cfg, target *ResourceRuntime, newLeader *Peer) Operator {
	transferLeader := newTransferLeaderOperator(target.meta.ID(), target.leaderPeer, newLeader)
	return newAggregationOp(cfg, target, transferLeader)
}

type transferLeaderOperator struct {
	Name      string `json:"name"`
	ID        uint64 `json:"id"`
	OldLeader *Peer  `json:"oldLeader"`
	NewLeader *Peer  `json:"newLeader"`
}

func newTransferLeaderOperator(id uint64, oldLeader, newLeader *Peer) Operator {
	return &transferLeaderOperator{
		Name:      "transfer_leader",
		ID:        id,
		OldLeader: oldLeader,
		NewLeader: newLeader,
	}
}

func (op *transferLeaderOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *transferLeaderOperator) ResourceID() uint64 {
	return op.ID
}

func (op *transferLeaderOperator) ResourceKind() ResourceKind {
	return LeaderKind
}

func (op *transferLeaderOperator) Do(info *ResourceRuntime) (*resourceHeartbeatRsp, bool) {
	// Check if operator is finished.
	if info.leaderPeer.ID == op.NewLeader.ID {
		return nil, true
	}

	return newChangeLeaderRsp(op.ID, op.NewLeader), false
}
