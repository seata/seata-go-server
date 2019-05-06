package core

import (
	"time"

	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/metrics"
)

type metricsListener struct {
	tc *cellTransactionCoordinator
}

func (m *metricsListener) OnEvent(e event) {
	now := time.Now()

	switch e.eventType {
	case registerG:
		g := e.data.(*meta.GlobalTransaction)
		metrics.ActionGCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionCreate,
			metrics.StatusSucceed,
			g.Name).Inc()
		metrics.ActiveGGauge.WithLabelValues(m.tc.idLabel).Set(float64(m.tc.doGetGCount()))
		break
	case registerGFailed:
		g := e.data.(meta.CreateGlobalTransaction)
		metrics.ActionGCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionCreate,
			metrics.StatusFailed,
			g.Name).Inc()
		break
	case registerB:
		b := e.data.(*meta.BranchTransaction)
		metrics.ActionBCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionCreate,
			metrics.StatusSucceed,
			b.Resource).Inc()
		break
	case registerBFailed:
		create := e.data.(meta.CreateBranchTransaction)
		metrics.ActionBCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionCreate,
			metrics.StatusFailed,
			create.ResourceID).Inc()
		break
	case reportB:
		b := e.data.(*meta.BranchTransaction)
		metrics.ActionBCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionReport,
			metrics.StatusSucceed,
			b.Resource).Inc()
		metrics.BDurationHistogram.WithLabelValues(m.tc.idLabel,
			metrics.PhaseOne).Observe(now.Sub(b.StartAtTime).Seconds())
		break
	case reportBFailed:
		report := e.data.(meta.ReportBranchStatus)
		metrics.ActionBCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionReport,
			metrics.StatusFailed,
			report.ResourceID).Inc()
		break
	case commitG:
		g := e.data.(*meta.GlobalTransaction)
		metrics.ActionGCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionCommit,
			metrics.StatusSucceed,
			g.Name).Inc()
		break
	case commitGFailed:
		metrics.ActionGCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionCommit,
			metrics.StatusFailed,
			"").Inc()
		break
	case rollbackG:
		g := e.data.(*meta.GlobalTransaction)
		metrics.ActionGCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionRollback,
			metrics.StatusSucceed,
			g.Name).Inc()
		break
	case rollbackGFailed:
		metrics.ActionGCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionRollback,
			metrics.StatusFailed,
			"").Inc()
		break
	case ackB:
		b := e.data.(*meta.BranchTransaction)
		metrics.ActionBCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionACK,
			metrics.StatusSucceed,
			b.Resource).Inc()
		metrics.BDurationHistogram.WithLabelValues(m.tc.idLabel,
			metrics.PhaseTwo).Observe(now.Sub(b.NotifyAtTime).Seconds())
		break
	case ackBFailed:
		metrics.ActionBCounter.WithLabelValues(m.tc.idLabel,
			metrics.ActionACK,
			metrics.StatusFailed,
			"").Inc()
		break
	case completeG:
		g := e.data.(*meta.GlobalTransaction)
		metrics.ActiveGGauge.WithLabelValues(m.tc.idLabel).Set(float64(m.tc.doGetGCount()))
		metrics.GDurationHistogram.WithLabelValues(m.tc.idLabel).Observe(now.Sub(g.StartAtTime).Seconds())
	}
}
