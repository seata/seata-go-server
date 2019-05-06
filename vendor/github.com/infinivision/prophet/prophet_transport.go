package prophet

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fagongzi/goetty"
)

const (
	typeResourceHeartbeatReq byte = iota
	typeResourceHeartbeatRsp
	typeContainerHeartbeatReq
	typeContainerHeartbeatRsp
	typeGetContainerReq
	typeGetContainerRsp
	typeAllocIDReq
	typeAllocIDRsp
	typeAskSplitReq
	typeAskSplitRsp
	typeErrorRsp
	typeInitWatcher
	typeEventNotify
)

// ResourceHeartbeatReq resource hb msg
type ResourceHeartbeatReq struct {
	Resource     Resource     `json:"-"`
	Data         []byte       `json:"data"`
	LeaderPeer   *Peer        `json:"leaderPeer"`
	DownPeers    []*PeerStats `json:"downPeers"`
	PendingPeers []*Peer      `json:"pendingPeers"`
}

// Init init
func (req *ResourceHeartbeatReq) Init(adapter Adapter) error {
	req.Resource = adapter.NewResource()
	err := req.Resource.Unmarshal(req.Data)
	if err != nil {
		return err
	}

	return nil
}

// Prepare prepare
func (req *ResourceHeartbeatReq) Prepare() error {
	data, err := req.Resource.Marshal()
	if err != nil {
		return err
	}

	req.Data = data
	return nil
}

type resourceHeartbeatRsp struct {
	ResourceID uint64         `json:"resourceID"`
	NewLeader  *Peer          `json:"newLeader"`
	Peer       *Peer          `json:"peer"`
	ChangeType ChangePeerType `json:"changeType"`
}

type getContainerReq struct {
	ID uint64 `json:"id"`
}

type getContainerRsp struct {
	Container Container `json:"container"`
}

func newChangeLeaderRsp(resourceID uint64, newLeader *Peer) *resourceHeartbeatRsp {
	return &resourceHeartbeatRsp{
		ResourceID: resourceID,
		NewLeader:  newLeader,
	}
}

func newChangePeerRsp(resourceID uint64, peer *Peer, changeType ChangePeerType) *resourceHeartbeatRsp {
	return &resourceHeartbeatRsp{
		ResourceID: resourceID,
		Peer:       peer,
		ChangeType: changeType,
	}
}

// ContainerHeartbeatReq container hb msg
type ContainerHeartbeatReq struct {
	Data               []byte    `json:"data"`
	StorageCapacity    uint64    `json:"storageCapacity"`
	StorageAvailable   uint64    `json:"storageAvailable"`
	LeaderCount        uint64    `json:"leaderCount"`
	ReplicaCount       uint64    `json:"replicaCount"`
	SendingSnapCount   uint64    `json:"sendingSnapCount"`
	ReceivingSnapCount uint64    `json:"receivingSnapCount"`
	ApplyingSnapCount  uint64    `json:"applyingSnapCount"`
	Busy               bool      `json:"busy"`
	Container          Container `json:"-"`
}

// Init init
func (req *ContainerHeartbeatReq) Init(adapter Adapter) error {
	req.Container = adapter.NewContainer()
	err := req.Container.Unmarshal(req.Data)
	if err != nil {
		return err
	}

	return nil
}

// Prepare prepare
func (req *ContainerHeartbeatReq) Prepare() error {
	data, err := req.Container.Marshal()
	if err != nil {
		return err
	}

	req.Data = data
	return nil
}

type containerHeartbeatRsp struct {
	Status string `json:"status"`
}

func newContainerHeartbeatRsp() Serializable {
	rsp := &containerHeartbeatRsp{}
	rsp.Status = "OK"

	return rsp
}

type errorRsp struct {
	Err string `json:"err"`
}

func newErrorRsp(err error) Serializable {
	rsp := &errorRsp{}
	rsp.Err = err.Error()

	return rsp
}

type allocIDReq struct {
}

type allocIDRsp struct {
	ID  uint64 `json:"id,omitempty"`
	Err error  `json:"err,omitempty"`
}

type askSplitReq struct {
	Resource Resource `json:"-"`
	Data     []byte   `json:"data,omitempty"`
}

// Init init
func (req *askSplitReq) Init(adapter Adapter) error {
	if len(req.Data) > 0 {
		req.Resource = adapter.NewResource()
		err := req.Resource.Unmarshal(req.Data)
		if err != nil {
			return err
		}
	}

	return nil
}

// Prepare prepare
func (req *askSplitReq) Prepare() error {
	if req.Resource != nil {
		data, err := req.Resource.Marshal()
		if err != nil {
			return err
		}

		req.Data = data
	}

	return nil
}

type askSplitRsp struct {
	Err        error    `json:"err,omitempty"`
	NewID      uint64   `json:"newID,omitempty"`
	NewPeerIDs []uint64 `json:"peerIDs,omitempty"`
}

// codec format: length(4bytes) + msgType(1bytes) + msg(length bytes)
type codec struct {
	adapter Adapter
}

func (c *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	var target Serializable
	var t byte

	if msg, ok := data.(*ResourceHeartbeatReq); ok {
		target = msg
		t = typeResourceHeartbeatReq
	} else if msg, ok := data.(*resourceHeartbeatRsp); ok {
		target = msg
		t = typeResourceHeartbeatRsp
	} else if msg, ok := data.(*ContainerHeartbeatReq); ok {
		target = msg
		t = typeContainerHeartbeatReq
	} else if msg, ok := data.(*containerHeartbeatRsp); ok {
		target = msg
		t = typeContainerHeartbeatRsp
	} else if msg, ok := data.(*allocIDReq); ok {
		target = msg
		t = typeAllocIDReq
	} else if msg, ok := data.(*allocIDRsp); ok {
		target = msg
		t = typeAllocIDRsp
	} else if msg, ok := data.(*askSplitReq); ok {
		target = msg
		t = typeAskSplitReq
	} else if msg, ok := data.(*askSplitRsp); ok {
		target = msg
		t = typeAskSplitRsp
	} else if msg, ok := data.(*errorRsp); ok {
		target = msg
		t = typeErrorRsp
	} else if msg, ok := data.(*InitWatcher); ok {
		target = msg
		t = typeInitWatcher
	} else if msg, ok := data.(*EventNotify); ok {
		target = msg
		t = typeEventNotify
	} else {
		return fmt.Errorf("not support msg: %+v", data)
	}

	if codecS, ok := target.(codecSerializable); ok {
		err := codecS.Prepare()
		if err != nil {
			return err
		}
	}

	value, err := json.Marshal(target)
	if err != nil {
		return err
	}

	out.WriteByte(t)
	out.Write(value)
	return nil
}

func (c *codec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	value := in.GetMarkedRemindData()
	in.MarkedBytesReaded()

	var msg Serializable
	t := value[0]

	switch t {
	case typeResourceHeartbeatReq:
		msg = &ResourceHeartbeatReq{}
		break
	case typeResourceHeartbeatRsp:
		msg = &resourceHeartbeatRsp{}
		break
	case typeContainerHeartbeatReq:
		msg = &ContainerHeartbeatReq{}
		break
	case typeContainerHeartbeatRsp:
		msg = &containerHeartbeatRsp{}
		break
	case typeAllocIDReq:
		msg = &allocIDReq{}
		break
	case typeAllocIDRsp:
		msg = &allocIDRsp{}
		break
	case typeAskSplitReq:
		msg = &askSplitReq{}
		break
	case typeAskSplitRsp:
		msg = &askSplitRsp{}
		break
	case typeErrorRsp:
		msg = &errorRsp{}
		break
	case typeInitWatcher:
		msg = &InitWatcher{}
		break
	case typeEventNotify:
		msg = &EventNotify{}
		break
	default:
		return false, nil, fmt.Errorf("unknown msg type")
	}

	err := json.Unmarshal(value[1:], msg)
	if err != nil {
		return false, nil, err
	}

	if codecS, ok := msg.(codecSerializable); ok {
		err = codecS.Init(c.adapter)
		if err != nil {
			return false, nil, err
		}
	}

	return true, msg, nil
}

func (p *Prophet) startListen() {
	go func() {
		err := p.tcpL.Start(p.doConnection)
		if err != nil {
			log.Fatalf("prophet: rpc listen at %s failed, errors:\n%+v",
				p.node.Addr,
				err)
		}
	}()

	<-p.tcpL.Started()
	log.Infof("prophet: start rpc listen at %s", p.node.Addr)
}

func (p *Prophet) doConnection(conn goetty.IOSession) error {
	if p.wn != nil {
		defer p.wn.clearWatcher(conn)
	}

	for {
		value, err := conn.Read()
		if err != nil {
			return err
		}

		if !p.isLeader() {
			conn.WriteAndFlush(newErrorRsp(fmt.Errorf("not leader")))
			continue
		}

		if msg, ok := value.(*ResourceHeartbeatReq); ok {
			rsp, err := p.handleResourceHeartbeat(msg)
			if err != nil {
				conn.WriteAndFlush(newErrorRsp(err))
				continue
			}
			conn.WriteAndFlush(rsp)
		} else if msg, ok := value.(*ContainerHeartbeatReq); ok {
			p.handleContainerHeartbeat(msg)
			conn.WriteAndFlush(newContainerHeartbeatRsp())
		} else if msg, ok := value.(*allocIDReq); ok {
			conn.WriteAndFlush(p.handleAllocID(msg))
		} else if msg, ok := value.(*askSplitReq); ok {
			conn.WriteAndFlush(p.handleAskSplit(msg))
		} else if msg, ok := value.(*InitWatcher); ok {
			p.wn.onInitWatcher(msg, conn)
		}
	}
}

func (p *Prophet) getLeaderClient() goetty.IOSession {
	for {
		l := p.leader
		addr := p.node.Addr
		if l != nil {
			addr = l.Addr
		}

		conn, err := p.createLeaderClient(addr)
		if err == nil {
			log.Infof("prophet: create leader connection to %s", addr)
			return conn
		}

		log.Errorf("prophet: create leader connection failed, errors: %+v", err)
		time.Sleep(time.Second)
	}
}

func (p *Prophet) createLeaderClient(leader string) (goetty.IOSession, error) {
	conn := goetty.NewConnector(leader,
		goetty.WithClientDecoder(goetty.NewIntLengthFieldBasedDecoder(p.bizCodec)),
		goetty.WithClientEncoder(goetty.NewIntLengthFieldBasedEncoder(p.bizCodec)),
		goetty.WithClientConnectTimeout(time.Second*10))
	_, err := conn.Connect()
	if err != nil {
		return nil, err
	}

	return conn, nil
}
