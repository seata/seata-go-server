package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/id"
	"seata.io/server/pkg/meta"
	"seata.io/server/pkg/storage/mem"
)

type testTransport struct {
}

func newTestTransport() *testTransport {
	return &testTransport{}
}

func (t *testTransport) Start() error                                           { return nil }
func (t *testTransport) Stop() error                                            { return nil }
func (t *testTransport) AsyncSend(string, meta.Notify, func(meta.Notify)) error { return nil }

type testElector struct {
	stopped                        bool
	becomeLeaderC, becomeFollowerC chan func()
}

func (e *testElector) Stop(id uint64) {
	e.stopped = true
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

func TestStop(t *testing.T) {
	elector := &testElector{}
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.Stop()
	assert.True(t, ctc.cmds.IsDisposed(), "check stop tc failed")
	assert.True(t, elector.stopped, "check stop tc failed")
}

func TestChangeLeaderTo(t *testing.T) {
	elector := &testElector{}
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.ChangeLeaderTo(100)
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check tc change to failed")
}

func TestActiveGCount(t *testing.T) {
	elector := &testElector{}
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	assert.Equal(t, 0, ctc.ActiveGCount(), "check tc active count failed")
}

func TestRegistryGlobalTransaction(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector), WithStorage(s), WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	var result uint64
	var resultErr error
	value := meta.CreateGlobalTransaction{}

	ctc.leader = false
	ctc.RegistryGlobalTransaction(value, func(gid uint64, err error) {
		result = gid
		resultErr = err
	})
	assert.Equal(t, uint64(0), result, "check registry g failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check registry g failed")

	ctc.leader = true
	ctc.RegistryGlobalTransaction(value, func(gid uint64, err error) {
		result = gid
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check registry g failed")
	ctc.leader = false
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check registry g failed")
	assert.Equal(t, uint64(0), result, "check registry g failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check registry g failed")

	ctc.leader = true
	ctc.RegistryGlobalTransaction(value, func(gid uint64, err error) {
		result = gid
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check registry g failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check registry g failed")
	assert.True(t, result > 0, "check registry g failed")
	assert.Nil(t, resultErr, "check registry g failed")
}

func TestRegistryBranchTransaction(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector), WithStorage(s), WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	var result uint64
	var resultErr error
	value := meta.CreateBranchTransaction{}

	ctc.leader = false
	ctc.RegistryBranchTransaction(value, func(bid uint64, err error) {
		result = bid
		resultErr = err
	})
	assert.Equal(t, uint64(0), result, "check registry b failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check registry b failed")

	ctc.leader = true
	ctc.RegistryBranchTransaction(value, func(bid uint64, err error) {
		result = bid
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check registry b failed")
	ctc.leader = false
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check registry b failed")
	assert.Equal(t, uint64(0), result, "check registry g failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check registry b failed")

	ctc.leader = true
	ctc.RegistryBranchTransaction(value, func(bid uint64, err error) {
		result = bid
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check registry b failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check registry b failed")
	assert.Equal(t, uint64(0), result, "check registry g failed")
	assert.NotNil(t, resultErr, "check registry b failed")

	gid := mustRegistryG(ctc)
	value.GID = gid

	ctc.leader = true
	ctc.RegistryBranchTransaction(value, func(bid uint64, err error) {
		result = bid
		resultErr = err
	})
	ctc.HandleEvent()
	assert.True(t, result > 0, "check registry g failed")
}

func TestReportBranchTransactionStatus(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector), WithStorage(s), WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	var resultErr error
	value := meta.ReportBranchStatus{Status: meta.BranchStatusPhaseOneDone}

	ctc.leader = false
	ctc.ReportBranchTransactionStatus(value, func(err error) {
		resultErr = err
	})
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check report b failed")

	ctc.leader = true
	ctc.ReportBranchTransactionStatus(value, func(err error) {
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check report b failed")
	ctc.leader = false
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check report b failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check report b failed")

	ctc.leader = true
	ctc.ReportBranchTransactionStatus(value, func(err error) {
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check report b failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check report b failed")
	assert.NotNil(t, resultErr, "check report b failed")

	gid := mustRegistryG(ctc)
	bid := mustRegistyB(ctc, gid)
	value.GID = gid
	value.BID = bid
	ctc.leader = true
	ctc.ReportBranchTransactionStatus(value, func(err error) {
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check report b failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check report b failed")
	assert.Nil(t, resultErr, "check report b failed")
}

func TestGlobalTransactionStatus(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tc, err := NewTransactionCoordinator(1, 2, nil, WithElector(elector), WithStorage(s), WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	var result meta.GlobalStatus
	var resultErr error

	ctc.leader = false
	ctc.GlobalTransactionStatus(1, func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check g status failed")

	ctc.leader = true
	ctc.GlobalTransactionStatus(1, func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check g status failed")
	ctc.leader = false
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check g status failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check g status failed")

	gid := mustRegistryG(ctc)
	ctc.leader = true
	ctc.GlobalTransactionStatus(gid, func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check g status failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check g status failed")
	assert.Nil(t, resultErr, "check g status failed")
	assert.Equal(t, meta.GlobalStatusBegin, result, "check g status failed")
}

func TestCommitGlobalTransaction(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tran := newTestTransport()
	tc, err := NewTransactionCoordinator(1, 2, tran,
		WithElector(elector),
		WithStorage(s),
		WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	var result meta.GlobalStatus
	var resultErr error

	ctc.leader = false
	ctc.CommitGlobalTransaction(1, "", func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check g commit failed")

	ctc.leader = true
	ctc.CommitGlobalTransaction(1, "", func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check g commit failed")
	ctc.leader = false
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check g commit failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check g commit failed")

	gid := mustRegistryG(ctc)
	bid := mustRegistyB(ctc, gid)
	mustReportB(ctc, gid, bid, meta.BranchStatusPhaseOneDone)
	ctc.leader = true
	ctc.CommitGlobalTransaction(gid, "", func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check g commit failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check g commit failed")
	assert.Nil(t, resultErr, "check g commit failed")
	assert.Equal(t, meta.GlobalStatusCommitting, result, "check g commit failed")
}

func TestRollbackGlobalTransaction(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tran := newTestTransport()
	tc, err := NewTransactionCoordinator(1, 2, tran,
		WithElector(elector),
		WithStorage(s),
		WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	var result meta.GlobalStatus
	var resultErr error

	ctc.leader = false
	ctc.RollbackGlobalTransaction(1, "", func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check g commit failed")

	ctc.leader = true
	ctc.RollbackGlobalTransaction(1, "", func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check g commit failed")
	ctc.leader = false
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check g commit failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check g commit failed")

	gid := mustRegistryG(ctc)
	bid := mustRegistyB(ctc, gid)
	mustReportB(ctc, gid, bid, meta.BranchStatusPhaseOneDone)
	ctc.leader = true
	ctc.RollbackGlobalTransaction(gid, "", func(status meta.GlobalStatus, err error) {
		result = status
		resultErr = err
	})
	assert.Equal(t, uint64(1), ctc.cmds.Len(), "check g commit failed")
	ctc.HandleEvent()
	assert.Equal(t, uint64(0), ctc.cmds.Len(), "check g commit failed")
	assert.Nil(t, resultErr, "check g commit failed")
	assert.Equal(t, meta.GlobalStatusRollbacking, result, "check g commit failed")
}

func TestBranchTransactionNotifyACK(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tran := newTestTransport()
	tc, err := NewTransactionCoordinator(1, 2, tran,
		WithElector(elector),
		WithStorage(s),
		WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	value := meta.NotifyACK{}
	gid := mustRegistryG(ctc)
	bid := mustRegistyB(ctc, gid)
	mustReportB(ctc, gid, bid, meta.BranchStatusPhaseOneDone)
	mustCommitG(ctc, gid)
	assert.Equal(t, 1, len(ctc.gids), "check ack failed")

	ctc.leader = true
	value.GID = gid
	value.BID = bid
	value.Status = meta.BranchStatusPhaseTwoCommitted
	value.Succeed = true
	ctc.BranchTransactionNotifyACK(value)
	ctc.HandleEvent()
	assert.Equal(t, 0, len(ctc.gids), "check ack failed")
}

func TestLockable(t *testing.T) {
	elector := &testElector{}
	s := mem.NewStorage()
	tran := newTestTransport()
	tc, err := NewTransactionCoordinator(1, 2, tran,
		WithElector(elector),
		WithStorage(s),
		WithIDGenerator(id.NewSnowflakeGenerator(1)))
	assert.Nilf(t, err, "create failed %+v", err)

	ctc := tc.(*defaultTC)
	ctc.reset()

	locks := []meta.LockKey{
		meta.LockKey{
			Namespace: "t1",
			Key:       "1",
		},
	}

	gid := mustRegistryG(ctc)
	bid := mustRegistyB(ctc, gid, meta.LockKey{
		Namespace: "t1",
		Key:       "1",
	})
	mustReportB(ctc, gid, bid, meta.BranchStatusPhaseOneDone)
	mustCommitG(ctc, gid)

	var result bool
	var resultErr error

	ctc.leader = false
	ctc.Lockable("", gid, locks, func(ok bool, err error) {
		result = ok
		resultErr = err
	})
	assert.True(t, !result, "check lockable failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check lockable failed")

	ctc.leader = true
	ctc.Lockable("", gid, locks, func(ok bool, err error) {
		result = ok
		resultErr = err
	})
	ctc.leader = false
	ctc.HandleEvent()
	assert.True(t, !result, "check lockable failed")
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check lockable failed")

	ctc.leader = true
	ctc.Lockable("", gid, locks, func(ok bool, err error) {
		result = ok
		resultErr = err
	})
	ctc.HandleEvent()
	assert.True(t, result, "check lockable failed")
	assert.Nil(t, resultErr, "check lockable failed")

	gid2 := mustRegistryG(ctc)
	ctc.leader = true
	ctc.Lockable("", gid2, locks, func(ok bool, err error) {
		result = ok
		resultErr = err
	})
	ctc.HandleEvent()
	assert.True(t, !result, "check lockable failed")
	assert.NotNil(t, resultErr, "check lockable failed")

	mustACK(ctc, gid, bid, meta.BranchStatusPhaseTwoCommitted)

	ctc.leader = true
	ctc.Lockable("", gid2, locks, func(ok bool, err error) {
		result = ok
		resultErr = err
	})
	ctc.HandleEvent()
	assert.True(t, result, "check lockable failed")
	assert.Nil(t, resultErr, "check lockable failed")
}

func mustRegistryG(ctc *defaultTC) uint64 {
	ctc.leader = true
	var gid uint64
	ctc.RegistryGlobalTransaction(meta.CreateGlobalTransaction{}, func(id uint64, err error) {
		gid = id
	})
	ctc.HandleEvent()
	return gid
}

func mustRegistyB(ctc *defaultTC, gid uint64, locks ...meta.LockKey) uint64 {
	ctc.leader = true
	var bid uint64
	ctc.RegistryBranchTransaction(meta.CreateBranchTransaction{GID: gid, LockKeys: locks}, func(id uint64, err error) {
		bid = id
	})
	ctc.HandleEvent()
	return bid
}

func mustReportB(ctc *defaultTC, gid, bid uint64, status meta.BranchStatus) {
	ctc.leader = true
	ctc.ReportBranchTransactionStatus(meta.ReportBranchStatus{GID: gid, BID: bid, Status: status}, func(err error) {
	})
	ctc.HandleEvent()
}

func mustCommitG(ctc *defaultTC, gid uint64) {
	ctc.leader = true
	ctc.CommitGlobalTransaction(gid, "", func(status meta.GlobalStatus, err error) {})
	ctc.HandleEvent()
}

func mustACK(ctc *defaultTC, gid, bid uint64, status meta.BranchStatus) {
	ctc.leader = true
	ctc.BranchTransactionNotifyACK(meta.NotifyACK{
		GID:     gid,
		BID:     bid,
		Status:  status,
		Succeed: true,
	})
	ctc.HandleEvent()
}
