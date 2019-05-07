package meta

import (
	"strings"
	"time"
)

// JSONResult json result
type JSONResult struct {
	Code  int         `json:"code"`
	Value interface{} `json:"value,omitempty"`
}

// Query query condition
type Query struct {
	Limit  uint64 `json:"limit"`
	After  uint64 `json:"after,omitempty"`
	Name   string `json:"name,omitempty"`
	Status int    `json:"status,omitempty"`
	Action int    `json:"action,omitempty"`
}

// Filter filter by conditions
func (q *Query) Filter(value *GlobalTransaction) bool {
	return q.filterByName(value) &&
		q.filterByStatus(value) &&
		q.filterByAction(value)
}

func (q *Query) filterByName(value *GlobalTransaction) bool {
	if q.Name == "" {
		return true
	}

	return strings.Index(value.Name, q.Name) != -1
}

func (q *Query) filterByStatus(value *GlobalTransaction) bool {
	if q.Status == -1 {
		return true
	}

	return byte(q.Status) == byte(value.Status)
}

func (q *Query) filterByAction(value *GlobalTransaction) bool {
	if q.Action == -1 {
		return true
	}

	return byte(q.Action) == byte(value.Action)
}

// CreateGlobalTransaction create a global
type CreateGlobalTransaction struct {
	Name    string        `json:"name"`
	Creator string        `json:"creator"`
	Proxy   string        `json:"proxy"`
	Timeout time.Duration `json:"timeout,omitempty"`
}

// CreateBranchTransaction create a branch transaction
type CreateBranchTransaction struct {
	ResourceID string     `json:"resourceId"`
	RMSID      string     `json:"rmSid"`
	GID        uint64     `json:"gid"`
	LockKeys   []LockKey  `json:"lockKeys"`
	BranchType BranchType `json:"branchType"`
}

// ReportBranchStatus report branch status
type ReportBranchStatus struct {
	ResourceID string       `json:"resourceId"`
	GID        uint64       `json:"gid"`
	BID        uint64       `json:"bid"`
	Status     BranchStatus `json:"status"`
	RMSID      string       `json:"from"`
}

// Summary summary for all transactions
type Summary struct {
	ActiveBrchNum  uint64 `json:"activeBrchNum"`
	ActiveTrxNum   uint64 `json:"activeTrxNum"`
	SuccessBrchNum uint64 `json:"successBrchNum"`
	FailedBrchNum  uint64 `json:"failedBrchNum"`
	SuccessTrxNum  uint64 `json:"successTrxNum"`
	FailedTrxNum   uint64 `json:"failedTrxNum"`
}
