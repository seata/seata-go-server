package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// FragmentGauge fragment count
	FragmentGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "seata",
			Subsystem: "sharding",
			Name:      "fragment_total",
			Help:      "Total number of Fragment.",
		}, []string{"role"})
)
