package core

import (
	"context"
	"testing"
)

type testElector struct {
	becomeLeaderC, becomeFollowerC chan func()
}

func (e *testElector) Stop(id uint64) {

}

func (e *testElector) CurrentLeader(id uint64) (uint64, error) {
	return 0, nil
}

func (e *testElector) ElectionLoop(ctx context.Context, id, leaderID uint64, nodeChecker func(uint64) bool, becomeLeader, becomeFollower func()) {
	for {
		select {
		case f := <-e.becomeLeaderC:
			f()
		case f := <-e.becomeFollowerC:
			f()
		}
	}
}

func (e *testElector) ChangeLeaderTo(id uint64, oldLeader, newLeader uint64) error {
	return nil
}

func TestRegistryRM(t *testing.T) {
	elector := &testElector{}
	tc, err := NewCellTransactionCoordinator(1, 2, nil, WithElector(elector))
	if err != nil {
		t.Errorf("create failed %+v", err)
		return
	}

	ctm := tc.(*cellTransactionCoordinator)
	if ctm.leader {
		t.Errorf("election failed, expect follower but leader")
		return
	}

	elector.becomeLeaderC <- ctm.becomeLeader

}
