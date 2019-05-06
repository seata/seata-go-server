package prophet

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
)

// Watcher watcher client
type Watcher struct {
	sync.RWMutex

	ops   uint64
	addrs []string
	conn  goetty.IOSession

	eventC chan *EventNotify
	stopC  chan struct{}
}

// NewWatcher returns a watcher for watch
func NewWatcher(addrs ...string) *Watcher {
	return &Watcher{
		addrs: addrs,
		stopC: make(chan struct{}, 1),
	}
}

// Watch watch event
func (w *Watcher) Watch(flag int) chan *EventNotify {
	w.Lock()
	defer w.Unlock()

	go w.watchDog(flag)
	w.eventC = make(chan *EventNotify)
	return w.eventC
}

// Stop stop watch
func (w *Watcher) Stop() {
	w.Lock()
	defer w.Unlock()

	w.stopC <- struct{}{}
	w.conn.Close()
	close(w.eventC)
}

func (w *Watcher) resetConn(flag int) {
	for {
		addr := w.next()
		if addr == "" {
			continue
		}

		c := &codec{}
		conn := goetty.NewConnector(addr,
			goetty.WithClientDecoder(goetty.NewIntLengthFieldBasedDecoder(c)),
			goetty.WithClientEncoder(goetty.NewIntLengthFieldBasedEncoder(c)),
			goetty.WithClientConnectTimeout(time.Second*10))
		_, err := conn.Connect()
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		err = conn.WriteAndFlush(&InitWatcher{
			Flag: flag,
		})
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		w.conn = conn
		return
	}
}

func (w *Watcher) watchDog(flag int) {
	for {
		select {
		case <-w.stopC:
			w.conn.Close()
			return
		default:
			w.resetConn(flag)
			w.startReadLoop()
		}
	}
}

func (w *Watcher) startReadLoop() {
	for {
		msg, err := w.conn.Read()
		if err != nil {
			w.conn.Close()
			return
		}

		if evt, ok := msg.(*EventNotify); ok {
			w.eventC <- evt
		} else {
			w.conn.Close()
			return
		}
	}
}

func (w *Watcher) next() string {
	l := uint64(len(w.addrs))
	if 0 >= l {
		return ""
	}

	return w.addrs[int(atomic.AddUint64(&w.ops, 1)%l)]
}
