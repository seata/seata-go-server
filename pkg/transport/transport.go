package transport

import (
	"errors"
	"hash/crc32"
	"sync"
	"time"

	"github.com/infinivision/taas/pkg/util"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

// Transport transport
type Transport interface {
	// Start start the transport
	Start() error
	// Stop stop the transport
	Stop() error
	// AsyncSend async send msg
	AsyncSend(string, meta.Notify, func(meta.Notify)) error
}

type message struct {
	resource string
	notify   meta.Notify
	cb       func(meta.Notify)
}

type transport struct {
	sync.RWMutex

	stopC                chan struct{}
	sendC                []chan message
	wg                   *sync.WaitGroup
	mask                 int
	resourceAddrDetecter func(uint64, string) (meta.ResourceManager, error)
	rpcFunc              func(meta.ResourceManager, meta.Notify) error
}

// NewTransport create a transport based on http
func NewTransport(workers int,
	resourceAddrDetecter func(uint64, string) (meta.ResourceManager, error),
	rpcFunc func(meta.ResourceManager, meta.Notify) error) Transport {
	return &transport{
		mask:                 workers - 1,
		resourceAddrDetecter: resourceAddrDetecter,
		rpcFunc:              rpcFunc,
	}
}

func (t *transport) AsyncSend(resource string, nt meta.Notify, cb func(meta.Notify)) error {
	t.RLock()
	if t.stopC == nil {
		t.RUnlock()
		return errors.New("transport stopped")
	}
	hash := int(crc32.ChecksumIEEE([]byte(resource)))
	t.sendC[t.mask&hash] <- message{
		resource: resource,
		notify:   nt,
		cb:       cb,
	}
	t.RUnlock()
	return nil
}

func (t *transport) Start() error {
	t.Lock()
	defer t.Unlock()

	if t.stopC != nil {
		return nil
	}

	t.wg = &sync.WaitGroup{}
	t.stopC = make(chan struct{})
	t.sendC = make([]chan message, 0, 0)
	for i := 0; i < t.mask+1; i++ {
		c := make(chan message, 1024)
		t.sendC = append(t.sendC, c)
		t.wg.Add(1)
		go func(q chan message, idx int) {
			t.readyToSend(q, idx)
		}(c, i)
	}

	return nil
}

func (t *transport) Stop() error {
	t.Lock()
	defer t.Unlock()

	if t.stopC == nil {
		return nil
	}

	close(t.stopC)
	t.wg.Wait()

	t.stopC = nil
	t.wg = nil

	return nil
}

func (t *transport) readyToSend(c chan message, index int) {
	log.Infof("transport[%d]: start", index)
	defer t.wg.Done()

	for {
		select {
		case <-t.stopC:
			log.Infof("transport[%d]: stopped", index)
			close(c)
			return
		case msg := <-c:
			log.Debugf("%s: ready",
				meta.TagBranchTransaction(msg.notify.XID.GID, msg.notify.BID, "notify"))
			rm, err := t.resourceAddrDetecter(msg.notify.XID.FragmentID, msg.resource)
			if err != nil {
				log.Warnf("%s: get RM[%s] failed with %+v",
					meta.TagBranchTransaction(msg.notify.XID.GID, msg.notify.BID, "notify"),
					msg.resource,
					err)
				util.DefaultTW.Schedule(time.Second*2, func(arg interface{}) {
					m := arg.(message)
					c <- m
				}, msg)
				break
			}

			if rm.ProxySID == "" {
				log.Fatalf("empty rm")
			}

			log.Debugf("%s: ready to %s",
				meta.TagBranchTransaction(msg.notify.XID.GID, msg.notify.BID, "notify"),
				rm.Tag())

			err = t.rpcFunc(rm, msg.notify)
			if err != nil {
				log.Warnf("%s: notify %s failed with %+v",
					meta.TagBranchTransaction(msg.notify.XID.GID, msg.notify.BID, "notify"),
					rm.Tag(),
					err)
				util.DefaultTW.Schedule(time.Second, func(arg interface{}) {
					m := arg.(message)
					c <- m
				}, msg)
				break
			}

			msg.cb(msg.notify)
			log.Infof("%s: %s succeed",
				meta.TagBranchTransaction(msg.notify.XID.GID, msg.notify.BID, "notify"),
				rm.Tag())
		}
	}
}
