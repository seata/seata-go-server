package metrics

import (
	"time"
	"unicode"

	"github.com/fagongzi/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushJob      string        `toml:"job" json:"job"`
	PushAddress  string        `toml:"address" json:"address"`
	PushInterval time.Duration `toml:"interval" json:"interval"`
}

func runesHasLowerNeighborAt(runes []rune, idx int) bool {
	if idx > 0 && unicode.IsLower(runes[idx-1]) {
		return true
	}
	if idx+1 < len(runes) && unicode.IsLower(runes[idx+1]) {
		return true
	}
	return false
}

func camelCaseToSnakeCase(str string) string {
	runes := []rune(str)
	length := len(runes)

	var ret []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && runesHasLowerNeighborAt(runes, i) {
			ret = append(ret, '_')
		}
		ret = append(ret, unicode.ToLower(runes[i]))
	}

	return string(ret)
}

// prometheusPushClient pushs metrics to Prometheus Pushgateway.
func prometheusPushClient(job, addr string, interval time.Duration) {
	for {
		err := push.FromGatherer(
			job, push.HostnameGroupingKey(),
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Errorf("push metrics to prometheus pushgateway failed with %+v", err)
		}

		time.Sleep(interval)
	}
}

// Push metircs in background.
func Push(cfg *MetricConfig) {
	if cfg.PushInterval == 0 || len(cfg.PushAddress) == 0 {
		log.Infof("disable prometheus push client")
		return
	}

	log.Info("start prometheus push client")

	interval := cfg.PushInterval
	go prometheusPushClient(cfg.PushJob, cfg.PushAddress, interval)
}
