package proxy

import (
	"sync"
	"sync/atomic"

	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/infinivision/prophet"
	"github.com/infinivision/taas/pkg/meta"
)

type router struct {
	sync.RWMutex

	op             uint64
	watcher        *prophet.Watcher
	stores         map[uint64]*meta.StoreMeta
	frags          map[uint64]*meta.Fragment
	availableFrags []uint64
	leaders        map[uint64]uint64 // fid->peer id
	opts           map[uint64]uint64
	transports     map[uint64]*transport
	initC          chan struct{}
	cb             func(uint64, *meta.RouteableMessage)
}

func newRouter(cb func(uint64, *meta.RouteableMessage), addrs ...string) *router {
	return &router{
		watcher:    prophet.NewWatcher(addrs...),
		stores:     make(map[uint64]*meta.StoreMeta),
		frags:      make(map[uint64]*meta.Fragment),
		leaders:    make(map[uint64]uint64),
		transports: make(map[uint64]*transport),
		initC:      make(chan struct{}, 1),
		cb:         cb,
	}
}

func (r *router) start() {
	go func() {
		c := r.watcher.Watch(prophet.EventFlagAll)
		for {
			evt, ok := <-c
			if !ok {
				return
			}

			switch evt.Event {
			case prophet.EventInit:
				r.updateAll(evt)
				log.Infof("router init complete")
				r.initC <- struct{}{}
			case prophet.EventResourceCreated:
				frag := parseFragment(evt.Value)
				r.addFragment(frag, true)
			case prophet.EventResourceChaned:
				frag := parseFragment(evt.Value)
				r.updateFragment(frag)
			case prophet.EventResourceLeaderChanged:
				frag, newLeader := evt.ReadLeaderChangerValue()
				r.updateLeader(frag, newLeader)
			case prophet.EventContainerCreated:
				store := parseStore(evt.Value)
				r.addStore(store, true)
			case prophet.EventContainerChanged:
				store := parseStore(evt.Value)
				r.updateStore(store)
			}
		}
	}()

	<-r.initC
}

func (r *router) retryDispatch(s *session, retry *meta.RetryNotLeaderMessage) {
	r.RLock()
	var store uint64
	if retry.NewLeader != 0 {
		store = retry.NewLeaderStore
	}

	if store == 0 {
		store = r.leaderStore(retry.FID)
	}

	if tran, ok := r.transports[store]; ok {
		var err error
		if nil != s.registerRMRequest() {
			err = tran.sent(s.toRouteableWithInternal(retry.FID, s.registerRMRequest()), retry.RetryData)
		} else {
			err = tran.sent(retry.RetryData)
		}

		if err != nil {
			log.Fatalf("router trans sent failed with %+v", err)
		}
	} else {
		log.Fatalf("router trans to store %d missing", store)
	}

	r.RUnlock()
}

func (r *router) dispatch(s *session, msgs []*meta.RouteableMessage) {
	r.RLock()
	for _, msg := range msgs {
		store := msg.ToStore
		if store == 0 {
			store = r.leaderStore(msg.FID)
		}

		if tran, ok := r.transports[store]; ok {
			var err error
			if s.isRegistered(store) || nil == s.registerRMRequest() {
				err = tran.sent(msg)
			} else {
				err = tran.sent(s.toRouteableWithInternal(msg.FID, s.registerRMRequest()), msg)
			}

			if err != nil {
				log.Fatalf("router trans sent failed with %+v", err)
			}
		} else {
			log.Fatalf("router trans to store %d missing", store)
		}
	}
	r.RUnlock()
}

func (r *router) peerStore(id, peerID uint64) uint64 {
	store := uint64(0)
	frag := r.frags[id]
	for _, p := range frag.Peers {
		if peerID == p.ID {
			store = p.ContainerID
			break
		}
	}
	return store
}

func (r *router) leaderStore(id uint64) uint64 {
	store := uint64(0)
	frag := r.frags[id]
	leader := r.leaders[id]
	for _, p := range frag.Peers {
		if leader == p.ID {
			store = p.ContainerID
			break
		}
	}
	return store
}

func (r *router) allocFragment() uint64 {
	r.RLock()
	id := r.availableFrags[atomic.AddUint64(&r.op, 1)%uint64(len(r.availableFrags))]
	r.RUnlock()
	return id
}

func (r *router) do(id uint64, cb func(*meta.Fragment)) {
	r.RLock()
	if frag, ok := r.frags[id]; ok {
		cb(frag)
	}
	r.RUnlock()
}

func (r *router) updateStore(store *meta.StoreMeta) {
	r.Lock()
	if _, ok := r.stores[store.ID]; !ok {
		log.Fatal("bugs: update a not exist store of event notify")
	}
	r.stores[store.ID] = store

	log.Debugf("[store-%d]: %+v changed",
		store.ID,
		store)
	r.Unlock()
}

func (r *router) addStore(store *meta.StoreMeta, lock bool) {
	if lock {
		r.Lock()
	}

	if _, ok := r.stores[store.ID]; ok {
		log.Fatal("bugs: add a exist store of event notify")
	}
	r.stores[store.ID] = store
	r.transports[store.ID] = newTransport(store.ID, store.ClientAddr, r)
	go r.transports[store.ID].start()

	log.Infof("[store-%d]: %+v added",
		store.ID,
		store)
	if lock {
		r.Unlock()
	}
}

func (r *router) updateLeader(frag, leader uint64) {
	r.Lock()
	if _, ok := r.frags[frag]; !ok {
		log.Fatal("bugs: update leader with a not exist frag of event notify")
	}
	r.leaders[frag] = leader

	log.Infof("[frag-%d]: leader change to peer %d",
		frag,
		leader)
	r.Unlock()
}

func (r *router) updateFragment(frag *meta.Fragment) {
	r.Lock()
	if _, ok := r.frags[frag.ID]; !ok {
		log.Fatal("bugs: update a not exist frag of event notify")
	}
	r.frags[frag.ID] = frag

	log.Infof("[frag-%d]: %+v updated",
		frag.ID,
		frag)
	r.Unlock()
}

func (r *router) addFragment(frag *meta.Fragment, lock bool) {
	if lock {
		r.Lock()
	}

	if _, ok := r.frags[frag.ID]; ok {
		log.Fatal("bugs: add a exist fragment of event notify")
	}
	r.frags[frag.ID] = frag
	r.availableFrags = append(r.availableFrags, frag.ID)

	log.Infof("[frag-%d]: %+v added",
		frag.ID,
		frag)
	if lock {
		r.Unlock()
	}
}

func (r *router) updateAll(evt *prophet.EventNotify) {
	r.Lock()
	r.stores = make(map[uint64]*meta.StoreMeta)
	r.frags = make(map[uint64]*meta.Fragment)
	r.availableFrags = make([]uint64, 0, 0)

	doFragFunc := func(data []byte, leader uint64) {
		frag := parseFragment(data)
		r.addFragment(frag, false)

		if leader > 0 {
			r.leaders[frag.ID] = leader
		}
	}

	doStoreFunc := func(data []byte) {
		r.addStore(parseStore(data), false)
	}
	evt.ReadInitEventValues(doFragFunc, doStoreFunc)
	r.Unlock()
}

func parseFragment(data []byte) *meta.Fragment {
	value := &meta.Fragment{}
	json.MustUnmarshal(value, data)
	return value
}

func parseStore(data []byte) *meta.StoreMeta {
	value := &meta.StoreMeta{}
	json.MustUnmarshal(value, data)
	return value
}
