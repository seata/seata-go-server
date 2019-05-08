package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.Register(ActionGCounter)
	prometheus.Register(ActiveGGauge)
	prometheus.Register(GDurationHistogram)
	prometheus.Register(ActionBCounter)
	prometheus.Register(BDurationHistogram)

	prometheus.Register(FragmentGauge)
	prometheus.Register(FragmentPeersGauge)
}
