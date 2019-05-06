package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ActionGCounter action on global count
	ActionGCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taas",
			Subsystem: "seata",
			Name:      "action_global_total",
			Help:      "Total number of global transcation actions made.",
		}, []string{"fragment", "action", "status", "name"})

	// ActiveGGauge active global transaction
	ActiveGGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taas",
		Subsystem: "seata",
		Name:      "global_active_total",
		Help:      "Total number of active global transaction.",
	}, []string{"fragment"})

	// GDurationHistogram global transaction duration time
	GDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taas",
			Subsystem: "seata",
			Name:      "global_duration_seconds",
			Help:      "Bucketed histogram of global processing duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"fragment"})

	// ActionBCounter action on branch
	ActionBCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taas",
			Subsystem: "seata",
			Name:      "action_branch_total",
			Help:      "Total number of action on branch transcation made.",
		}, []string{"fragment", "action", "status", "resource"})

	// BDurationHistogram branch transaction duration time with all phase
	BDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "taas",
			Subsystem: "seata",
			Name:      "branch_duration_seconds",
			Help:      "Bucketed histogram of branch processing duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"fragment", "phase"})
)
