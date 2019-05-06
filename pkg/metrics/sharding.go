package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// FragmentCounter fragment count
	FragmentCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "taas",
			Subsystem: "sharding",
			Name:      "fragment_total",
			Help:      "Total number of Fragment.",
		})

	// FragmentLeaderCounter fragment count
	FragmentLeaderCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "taas",
			Subsystem: "sharding",
			Name:      "leader_total",
			Help:      "Total number of Fragment leader.",
		})

	// FragmentPeersGauge fragment peers value
	FragmentPeersGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "taas",
		Subsystem: "seata",
		Name:      "peers_total",
		Help:      "Total number of peers per Fragment.",
	}, []string{"fragment"})
)
