package metrics

const (
	// StatusSucceed failed
	StatusSucceed = "succeed"
	// StatusFailed failed
	StatusFailed = "failed"

	// ActionCreate create new branch transaction
	ActionCreate = "new"
	// ActionReport report the branch transaction status on phase one
	ActionReport = "report"
	// ActionCommit commit the global transaction
	ActionCommit = "commit"
	// ActionRollback rollback the global transaction
	ActionRollback = "rollback"
	// ActionACK report the branch transaction status on phase two
	ActionACK = "ack"

	// PhaseOne phase one
	PhaseOne = "one"
	// PhaseTwo phase two
	PhaseTwo = "two"
)

const (
	// RoleLeader leader fragment
	RoleLeader = "leader"
	// RoleFollower follower fragment
	RoleFollower = "follower"
)
