package prophet

import (
	"fmt"
)

// ChangePeerType change peer type
type ChangePeerType byte

var (
	// AddPeer add peer
	AddPeer = ChangePeerType(0)
	// RemovePeer remove peer
	RemovePeer = ChangePeerType(1)
)

func newAddPeerAggregationOp(cfg *Cfg, target *ResourceRuntime, peer *Peer) Operator {
	addPeerOp := newAddPeerOp(target.meta.ID(), peer)
	return newAggregationOp(cfg, target, addPeerOp)
}

func newTransferPeerAggregationOp(cfg *Cfg, target *ResourceRuntime, oldPeer, newPeer *Peer) Operator {
	addPeer := newAddPeerOp(target.meta.ID(), newPeer)
	removePeer := newRemovePeerOp(target.meta.ID(), oldPeer)
	return newAggregationOp(cfg, target, addPeer, removePeer)
}

func newAddPeerOp(id uint64, peer *Peer) *changePeerOperator {
	return &changePeerOperator{
		Name: "add_peer",
		ID:   id,
		Peer: peer,
		Type: AddPeer,
	}
}

func newRemovePeerOp(id uint64, peer *Peer) *changePeerOperator {
	return &changePeerOperator{
		Name: "remove_peer",
		ID:   id,
		Peer: peer,
		Type: RemovePeer,
	}
}

// changePeerOperator is sub operator of resourceOperator
type changePeerOperator struct {
	Name string         `json:"name"`
	ID   uint64         `json:"id"`
	Type ChangePeerType `json:"type"`
	Peer *Peer          `json:"peer"`
}

func (op *changePeerOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *changePeerOperator) ResourceID() uint64 {
	return op.ID
}

func (op *changePeerOperator) ResourceKind() ResourceKind {
	return ReplicaKind
}

func (op *changePeerOperator) Do(target *ResourceRuntime) (*resourceHeartbeatRsp, bool) {
	// Check if operator is finished.
	switch op.Type {
	case AddPeer:
		if target.GetPendingPeer(op.Peer.ID) != nil {
			// Peer is added but not finished.
			return nil, false
		}
		if target.GetPeer(op.Peer.ID) != nil {
			// Peer is added and finished.
			return nil, true
		}
	case RemovePeer:
		if target.GetPeer(op.Peer.ID) == nil {
			// Peer is removed.
			return nil, true
		}
	}

	return newChangePeerRsp(op.ID, op.Peer, op.Type), false
}
