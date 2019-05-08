package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// FragmentGauge fragment count
	FragmentGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "taas",
			Subsystem: "sharding",
			Name:      "fragment_total",
			Help:      "Total number of Fragment.",
		}, []string{"role"})

	// FragmentPeersGauge fragment peers value
	FragmentPeersGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taas",
		Subsystem: "seata",
		Name:      "peers_total",
		Help:      "Total number of peers per Fragment.",
	}, []string{"fragment"})
)
