package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ClientGCounter global count in client
	ClientGCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taas",
			Subsystem: "client",
			Name:      "global_total",
			Help:      "Total number of global transcation made in client.",
		}, []string{"status"})

	// ClientGRegisterDurationHistogram global transaction register duration in client
	ClientGRegisterDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "taas",
			Subsystem: "client",
			Name:      "global_register_duration_seconds",
			Help:      "Bucketed histogram of global register processing duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	// ClientGDurationHistogram global transaction duration from begin to complete in client
	ClientGDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "taas",
			Subsystem: "client",
			Name:      "global_duration_seconds",
			Help:      "Bucketed histogram of global processing duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	// ClientBCounter branch count in client
	ClientBCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "taas",
			Subsystem: "client",
			Name:      "branch_total",
			Help:      "Total number of branch transcation made in client.",
		}, []string{"status"})
)
