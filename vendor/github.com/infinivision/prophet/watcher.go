package prophet

import (
	"sync"

	"github.com/fagongzi/goetty"
)

var (
	// EventInit event init
	EventInit = 1 << 1
	// EventResourceCreated event resource created
	EventResourceCreated = 1 << 2
	// EventResourceLeaderChanged event resource leader changed
	EventResourceLeaderChanged = 1 << 3
	// EventResourceChaned event resource changed
	EventResourceChaned = 1 << 4
	// EventContainerCreated  event container create
	EventContainerCreated = 1 << 5
	// EventContainerChanged  event container create
	EventContainerChanged = 1 << 6

	// EventFlagResource all resource event
	EventFlagResource = EventResourceCreated | EventResourceLeaderChanged | EventResourceChaned
	// EventFlagContainer all container event
	EventFlagContainer = EventContainerCreated | EventContainerChanged
	// EventFlagAll all event
	EventFlagAll = 0xffffffff
)

// MatchEvent returns the flag has the target event
func MatchEvent(event, flag int) bool {
	return event == 0 || event&flag != 0
}

func (p *Prophet) notifyEvent(event *EventNotify) {
	if event == nil {
		return
	}

	if p.isLeader() {
		p.wn.onEvent(event)
	}
}

// InitWatcher init watcher
type InitWatcher struct {
	Flag int `json:"flag"`
}

// EventNotify event notify
type EventNotify struct {
	Event int    `json:"event"`
	Value []byte `json:"value"`
}

func newInitEvent(rt *Runtime) (*EventNotify, error) {
	value := goetty.NewByteBuf(512)

	snap := rt.snap()
	value.WriteInt(len(snap.resources))
	value.WriteInt(len(snap.containers))

	for _, v := range snap.resources {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}
		value.WriteInt(len(data) + 8)
		value.Write(data)
		if id, ok := snap.leaders[v.ID()]; ok {
			value.WriteUint64(id)
		} else {
			value.WriteUint64(0)
		}
	}

	for _, v := range snap.containers {
		data, err := v.Marshal()
		if err != nil {
			return nil, err
		}
		value.WriteInt(len(data))
		value.Write(data)
	}

	_, data, _ := value.ReadAll()
	nt := &EventNotify{
		Event: EventInit,
		Value: data,
	}
	value.Release()
	return nt, nil
}

func newResourceEvent(event int, target Resource) *EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return nil
	}

	return &EventNotify{
		Event: event,
		Value: value,
	}
}

func newContainerEvent(event int, target Container) *EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return nil
	}

	return &EventNotify{
		Event: event,
		Value: value,
	}
}

func newLeaderChangerEvent(target, leader uint64) *EventNotify {
	value := goetty.NewByteBuf(16)
	value.WriteUint64(target)
	value.WriteUint64(leader)

	_, data, _ := value.ReadAll()
	nt := &EventNotify{
		Event: EventResourceLeaderChanged,
		Value: data,
	}
	value.Release()
	return nt
}

// ReadInitEventValues read all resource info
func (evt *EventNotify) ReadInitEventValues(resourceF func([]byte, uint64), containerF func([]byte)) {
	if len(evt.Value) == 0 {
		return
	}

	buf := goetty.NewByteBuf(len(evt.Value))
	buf.Write(evt.Value)
	rn, _ := buf.ReadInt()
	cn, _ := buf.ReadInt()

	for i := 0; i < rn; i++ {
		size, _ := buf.ReadInt()
		_, data, _ := buf.ReadBytes(size - 8)
		leader, _ := buf.ReadUInt64()
		resourceF(data, leader)
	}

	for i := 0; i < cn; i++ {
		size, _ := buf.ReadInt()
		_, data, _ := buf.ReadBytes(size)
		containerF(data)
	}

	buf.Release()
	return
}

// ReadLeaderChangerValue returns the target resource and the new leader
// returns resourceid, newleaderid
func (evt *EventNotify) ReadLeaderChangerValue() (uint64, uint64) {
	if len(evt.Value) == 0 {
		return 0, 0
	}
	buf := goetty.NewByteBuf(len(evt.Value))
	buf.Write(evt.Value)
	resourceID, _ := buf.ReadUInt64()
	newLeaderID, _ := buf.ReadUInt64()
	buf.Release()

	return resourceID, newLeaderID
}

type watcher struct {
	info *InitWatcher
	conn goetty.IOSession
}

func (wt *watcher) notify(evt *EventNotify) {
	if MatchEvent(evt.Event, wt.info.Flag) {
		err := wt.conn.WriteAndFlush(evt)
		if err != nil {
			wt.conn.Close()
		}
	}
}

type watcherNotifier struct {
	sync.RWMutex

	watchers *sync.Map
	eventC   chan *EventNotify
	rt       *Runtime
}

func newWatcherNotifier(rt *Runtime) *watcherNotifier {
	return &watcherNotifier{
		watchers: &sync.Map{},
		eventC:   make(chan *EventNotify, 256),
		rt:       rt,
	}
}

func (wn *watcherNotifier) onEvent(evt *EventNotify) {
	wn.RLock()
	if wn.eventC != nil {
		wn.eventC <- evt
	}
	wn.RUnlock()
}

func (wn *watcherNotifier) onInitWatcher(msg *InitWatcher, conn goetty.IOSession) {
	wn.Lock()
	defer wn.Unlock()

	log.Infof("prophet: new watcher %s added", conn.RemoteIP())

	if wn.eventC != nil {
		if MatchEvent(EventInit, msg.Flag) {
			nt, err := newInitEvent(wn.rt)
			if err != nil {
				log.Errorf("prophet: marshal init notify failed, errors:%+v", err)
				conn.Close()
				return
			}

			err = conn.WriteAndFlush(nt)
			if err != nil {
				log.Errorf("prophet: notify to %s failed, errors:%+v",
					conn.RemoteIP(),
					err)
				conn.Close()
				return
			}
		}

		wn.watchers.Store(conn.ID(), &watcher{
			info: msg,
			conn: conn,
		})
	}
}

func (wn *watcherNotifier) clearWatcher(conn goetty.IOSession) {
	wn.Lock()
	log.Infof("prophet: clear watcher %s", conn.RemoteIP())
	wn.watchers.Delete(conn.ID())
	wn.Unlock()
}

func (wn *watcherNotifier) stop() {
	wn.Lock()
	close(wn.eventC)
	wn.eventC = nil
	wn.watchers.Range(func(key, value interface{}) bool {
		wn.watchers.Delete(key)
		value.(*watcher).conn.Close()
		return true
	})
	wn.Unlock()
	log.Infof("prophet: watcher notifyer stopped")
}

func (wn *watcherNotifier) start() {
	for {
		evt, ok := <-wn.eventC
		if !ok {
			log.Infof("prophet: watcher notifer exited")
			return
		}

		log.Debugf("prophet: new event: %+v", evt)
		wn.watchers.Range(func(key, value interface{}) bool {
			wt := value.(*watcher)
			wt.notify(evt)
			return true
		})
	}
}

type snap struct {
	resources  []Resource
	containers []Container
	leaders    map[uint64]uint64
}

func (rc *Runtime) snap() *snap {
	rc.RLock()
	defer rc.RUnlock()

	value := &snap{
		resources:  make([]Resource, len(rc.resources), len(rc.resources)),
		containers: make([]Container, len(rc.containers), len(rc.containers)),
		leaders:    make(map[uint64]uint64),
	}

	idx := 0
	for _, c := range rc.containers {
		value.containers[idx] = c.meta.Clone()
		idx++
	}

	idx = 0
	for _, r := range rc.resources {
		value.resources[idx] = r.meta.Clone()
		if r.leaderPeer != nil {
			value.leaders[r.meta.ID()] = r.leaderPeer.ID
		}
		idx++
	}

	return value
}
