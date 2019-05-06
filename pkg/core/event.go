package core

type eventType int

var (
	registerG       = eventType(0)
	registerGFailed = eventType(1)
	registerB       = eventType(2)
	registerBFailed = eventType(3)
	reportB         = eventType(4)
	reportBFailed   = eventType(5)
	commitG         = eventType(6)
	commitGFailed   = eventType(7)
	rollbackG       = eventType(8)
	rollbackGFailed = eventType(9)
	ackB            = eventType(10)
	ackBFailed      = eventType(11)
	completeG       = eventType(12)
)

type event struct {
	eventType eventType
	data      interface{}
}

type eventListener interface {
	OnEvent(e event)
}

func (tc *cellTransactionCoordinator) initEvent() {
	tc.eventListeners = make([]eventListener, 0)
	tc.eventListeners = append(tc.eventListeners, tc.metricsListener)
}

func (tc *cellTransactionCoordinator) publishEvent(e event) {
	for _, lister := range tc.eventListeners {
		lister.OnEvent(e)
	}
}
