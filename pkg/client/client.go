package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/util"
)

var (
	stop = struct{}{}

	batch int64 = 32
)

// ResourceHandler resource handler
type ResourceHandler interface {
	OnBranchCommit(xid meta.FragmentXID, bid uint64)
	OnBranchRollback(xid meta.FragmentXID, bid uint64)
}

// Cfg client cfg
type Cfg struct {
	Seq               uint64
	Addrs             []string
	HeartbeatDuration time.Duration
	Timeout           time.Duration
	Handler           ResourceHandler

	ApplicationID, Version, ApplicationData string
	Resources                               []string
}

// Client seata client
type Client struct {
	cfg  Cfg
	op   uint64
	size uint64
	seq  uint64

	regiterReq   *meta.RegisterRMRequest
	queues       map[string]*task.Queue
	asyncContext sync.Map
}

// NewClient returns a seata client
func NewClient(cfg Cfg) *Client {
	c := &Client{
		cfg:        cfg,
		size:       uint64(len(cfg.Addrs)),
		regiterReq: &meta.RegisterRMRequest{},
		queues:     make(map[string]*task.Queue),
		seq:        cfg.Seq,
	}

	for _, addr := range cfg.Addrs {
		c.queues[addr] = task.New(128)
		go c.sentTask(addr, c.queues[addr])
	}

	c.regiterReq.ApplicationID = cfg.ApplicationID
	c.regiterReq.Version = cfg.Version
	c.regiterReq.ExtraData = cfg.ApplicationData
	c.regiterReq.ResourceIDs = strings.Join(cfg.Resources, ",")
	return c
}

// CreateBatch  create batch operation
func (c *Client) CreateBatch() Batch {
	return newBatch()
}

// CommitBatch commit batch
func (c *Client) CommitBatch(batch Batch) ([]meta.Message, error) {
	ops := batch.(*batchOperation)
	if len(ops.ops.Msgs) == 0 {
		return nil, fmt.Errorf("empty batch operations")
	}

	for range ops.ops.Msgs {
		ops.ops.MsgIDs = append(ops.ops.MsgIDs, c.nextID())
	}

	ctx := c.newTimeoutCtx(c.nextID(), ops.ops)
	ctx.batch = true
	c.addToSend(ctx)

	data, err := ctx.get()
	if err != nil {
		return nil, err
	}

	rsps := data.(*meta.MergeResultMessage)
	if len(rsps.Msgs) == 0 {
		log.Fatalf("respose 0 for %d, %+v", ctx.id, ops.ops)
	}
	return rsps.Msgs, nil
}

// CreateGlobal register a global transaction
func (c *Client) CreateGlobal(name string, timeout time.Duration) (meta.FragmentXID, error) {
	req := meta.AcquireGlobalBeginRequest()
	req.Timeout = int(timeout / time.Millisecond)
	req.TransactionName = name

	ctx := c.newTimeoutCtx(c.nextID(), req)
	c.addToSend(ctx)

	data, err := ctx.get()
	if err != nil {
		return meta.FragmentXID{}, err
	}

	rsp := data.(*meta.GlobalBeginResponse)
	if !rsp.Succeed() {
		return meta.FragmentXID{}, fmt.Errorf("register global failed with code %d, %s",
			rsp.ResultCode,
			rsp.Msg)
	}

	return rsp.XID, nil
}

// RegisterBranch register a branch transaction
func (c *Client) RegisterBranch(xid meta.FragmentXID, resource string, branchType meta.BranchType, locks string, applicationData string) (uint64, error) {
	req := meta.AcquireBranchRegisterRequest()
	req.BranchType = branchType
	req.XID = xid
	req.ResourceID = resource
	req.LockKey = locks
	req.ApplicationData = applicationData

	ctx := c.newTimeoutCtx(c.nextID(), req)
	c.addToSend(ctx)

	data, err := ctx.get()
	if err != nil {
		return 0, err
	}

	rsp := data.(*meta.BranchRegisterResponse)
	if !rsp.Succeed() {
		return 0, fmt.Errorf("register branch failed with code %d, %s",
			rsp.ResultCode,
			rsp.Msg)
	}

	return rsp.BranchID, nil
}

// ReportBranchStatus report branch status at phase one
func (c *Client) ReportBranchStatus(xid meta.FragmentXID, bid uint64, status meta.BranchStatus,
	branchType meta.BranchType, resource string, applicationData string) error {
	req := meta.AcquireBranchReportRequest()
	req.XID = xid
	req.BranchID = bid
	req.BranchStatus = status
	req.ResourceID = resource
	req.BranchType = branchType
	req.ApplicationData = applicationData

	ctx := c.newTimeoutCtx(c.nextID(), req)
	c.addToSend(ctx)

	data, err := ctx.get()
	if err != nil {
		return err
	}

	rsp := data.(*meta.BranchReportResponse)
	if !rsp.Succeed() {
		return fmt.Errorf("report branch failed with code %d, %s",
			rsp.ResultCode,
			rsp.Msg)
	}

	return nil
}

// CommitGlobal commit global transaction
func (c *Client) CommitGlobal(xid meta.FragmentXID, extraData string) error {
	req := meta.AcquireGlobalCommitRequest()
	req.XID = xid
	req.ExtraData = extraData

	ctx := c.newTimeoutCtx(c.nextID(), req)
	c.addToSend(ctx)

	data, err := ctx.get()
	if err != nil {
		return err
	}

	rsp := data.(*meta.GlobalCommitResponse)
	if !rsp.Succeed() {
		return fmt.Errorf("commit global failed with code %d, %s",
			rsp.ResultCode,
			rsp.Msg)
	}

	return nil
}

// BranchCommitACK ack branch commit
func (c *Client) BranchCommitACK(xid meta.FragmentXID, bid uint64, status meta.BranchStatus) {
	req := meta.AcquireBranchCommitResponse()
	req.XID = xid
	req.BranchID = bid
	req.BranchStatus = status
	req.ResultCode = meta.Succeed

	c.addToSend(&ctx{msg: req})
}

// BranchRollbackACK ack branch rollback
func (c *Client) BranchRollbackACK(xid meta.FragmentXID, bid uint64, status meta.BranchStatus) {
	req := meta.AcquireBranchRollbackResponse()
	req.XID = xid
	req.BranchID = bid
	req.BranchStatus = status
	req.ResultCode = meta.Succeed

	c.addToSend(&ctx{msg: req})
}

// RollbackGlobal rollback global transaction
func (c *Client) RollbackGlobal(xid meta.FragmentXID, extraData string) error {
	req := meta.AcquireGlobalRollbackRequest()
	req.XID = xid
	req.ExtraData = extraData

	ctx := c.newTimeoutCtx(c.nextID(), req)
	c.addToSend(ctx)

	data, err := ctx.get()
	if err != nil {
		return err
	}

	rsp := data.(*meta.GlobalRollbackResponse)
	if !rsp.Succeed() {
		return fmt.Errorf("rollback global failed with code %d, %s",
			rsp.ResultCode,
			rsp.Msg)
	}

	return nil
}

func (c *Client) sentTask(addr string, q *task.Queue) {
	items := make([]interface{}, batch, batch)
	conn := c.createConn(addr)
	defer conn.Close()

	go func() {
		for {
			time.Sleep(c.cfg.HeartbeatDuration)
			err := c.doHB(addr, conn)
			if err != nil {
				return
			}
		}
	}()

	for {
		n, err := q.Get(batch, items)
		if err != nil {
			log.Infof("%s sent task stopped", addr)
			return
		}

		available := conn.IsConnected()
		if !available {
			available = c.prepare(addr, conn)
		}

		wrapped := meta.AcquireMergedWarpMessage()
		for i := int64(0); i < n; i++ {
			item := items[i]
			if item == stop {
				log.Infof("%s sent task stopped", addr)
				return
			}

			cc := item.(*ctx)
			cc.sended = time.Now()
			if !available {
				c.addToSendWithExclude(cc, addr)
				continue
			}

			if !cc.hasCallback() || cc.batch {
				err := conn.WriteAndFlush(c.buildRPCMessage(cc.id, cc.msg))
				if err != nil {
					c.addToSendWithExclude(cc, addr)
					available = false
					conn.Close()
					continue
				}
			} else {
				wrapped.MsgIDs = append(wrapped.MsgIDs, cc.id)
				wrapped.Msgs = append(wrapped.Msgs, cc.msg)
			}
		}

		if available && len(wrapped.Msgs) > 0 {
			msg := c.buildRPCMessage(0, wrapped)
			c.addAsyncContext(c.newTimeoutCtx(msg.Header.MsgID, wrapped))
			err := conn.WriteAndFlush(msg)
			if err == nil {
				continue
			}

			available = false
			conn.Close()
			for i := int64(0); i < n; i++ {
				c.addToSendWithExclude(items[i].(*ctx), addr)
			}
		}
	}
}

func (c *Client) startReadLoop(addr string, conn goetty.IOSession) {
	log.Infof("start read loop from %s", addr)
	for {
		data, err := conn.Read()
		if err != nil {
			log.Errorf("exit read loop from %s with %+v",
				addr,
				err)
			conn.Close()
			return
		}

		msg := data.(*meta.RPCMessage)
		switch msg.Header.MsgType {
		case meta.TypeSeataMergeResult:
			wrapped := msg.Body.(*meta.MergeResultMessage)
			if value, ok := c.asyncContext.Load(msg.Header.MsgID); ok {
				c.asyncContext.Delete(msg.Header.MsgID)
				cc := value.(*ctx)
				if cc.batch {
					cc.completeC <- wrapped
				} else {
					ids := cc.msg.(*meta.MergedWarpMessage).MsgIDs
					for idx, msg := range wrapped.Msgs {
						c.onReceived(ids[idx], msg)
					}
				}
			}
			break
		case meta.TypeBranchCommit:
			req := msg.Body.(*meta.BranchCommitRequest)
			c.cfg.Handler.OnBranchCommit(req.XID, req.BranchID)
			break
		case meta.TypeBranchRollback:
			req := msg.Body.(*meta.BranchRollbackRequest)
			c.cfg.Handler.OnBranchRollback(req.XID, req.BranchID)
			break
		}
	}
}

func (c *Client) prepare(addr string, conn goetty.IOSession) bool {
	_, err := conn.Connect()
	if err != nil {
		log.Errorf("connect to %s failed with %+v",
			addr,
			err)
		return false
	}

	msg := c.buildRPCMessage(0, c.regiterReq)
	data, err := c.syncCall(msg, addr, conn)
	if err != nil {
		log.Errorf("read register RM response from %s failed with %+v",
			addr,
			err)
		conn.Close()
		return false
	}

	if data.Header.MsgType != meta.TypeRegRMResult {
		log.Fatalf("bugs: unexpect rsp type %d",
			data.Header.MsgType)
	}

	rsp, ok := data.Body.(*meta.RegisterRMResponse)
	if !ok {
		log.Fatalf("bugs: unexpect rsp for register (%T)%+v",
			rsp,
			rsp)
	}

	log.Infof("RM register succ !!!")
	go c.startReadLoop(addr, conn)
	return true
}

func (c *Client) syncCall(req *meta.RPCMessage, addr string, conn goetty.IOSession) (*meta.RPCMessage, error) {
	err := conn.WriteAndFlush(req)
	if err != nil {
		return nil, err
	}

	data, err := conn.ReadTimeout(c.cfg.Timeout)
	if err != nil {
		return nil, err
	}

	return data.(*meta.RPCMessage), nil
}

func (c *Client) createConn(addr string) goetty.IOSession {
	return goetty.NewConnector(addr,
		goetty.WithClientDecoder(meta.SeataDecoder),
		goetty.WithClientEncoder(meta.SeataEncoder),
		goetty.WithClientConnectTimeout(time.Second*5))
}

func (c *Client) doHB(addr string, conn goetty.IOSession) error {
	return c.queues[addr].Put(&ctx{msg: meta.SeataHB})
}

func (c *Client) addToSend(msg *ctx) {
	msg.added = time.Now()
	err := c.queues[c.next()].Put(msg)
	if err != nil {
		log.Fatalf("add %d failed with %+v", msg.id, err)
	}
}

func (c *Client) addToSendWithExclude(msg *ctx, exclude string) {
	if len(c.cfg.Addrs) == 1 {
		c.addToSend(msg)
		return
	}

	for {
		addr := c.next()
		if addr != exclude {
			c.queues[addr].Put(msg)
			return
		}
	}
}

func (c *Client) next() string {
	return c.cfg.Addrs[atomic.AddUint64(&c.op, 1)%c.size]
}

func (c *Client) nextID() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}

func (c *Client) onReceived(id uint64, msg meta.Message) {
	if cc, ok := c.asyncContext.Load(id); ok {
		cc.(*ctx).completeC <- msg
		c.asyncContext.Delete(id)
	}
}

func (c *Client) addAsyncContext(cc *ctx) {
	c.asyncContext.Store(cc.id, cc)
	util.DefaultTW.Schedule(c.cfg.Timeout, c.onTimeout, cc.id)
}

func (c *Client) onTimeout(arg interface{}) {
	c.asyncContext.Delete(arg.(uint64))
}

func (c *Client) newTimeoutCtx(id uint64, msg meta.Message) *ctx {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), c.cfg.Timeout)
	cc := &ctx{
		id:         id,
		msg:        msg,
		completeC:  make(chan meta.Message, 1),
		cancel:     cancel,
		timeoutCtx: timeoutCtx,
	}

	c.addAsyncContext(cc)
	return cc
}

func (c *Client) buildRPCMessage(seq uint64, req meta.Message) *meta.RPCMessage {
	msg := meta.AcquireRPCMessage()
	msg.Header.Flag = meta.FlagSeataCodec | meta.FlagRequest
	msg.Header.MsgType = req.Type()
	if seq == 0 {
		msg.Header.MsgID = c.nextID()
	} else {
		msg.Header.MsgID = seq
	}
	msg.Body = req

	if msg.Header.MsgType == meta.TypeHeartbeat {
		msg.Header.Flag |= meta.FlagHeartbeat
	}

	return msg
}

type ctx struct {
	id         uint64
	batch      bool
	msg        meta.Message
	completeC  chan meta.Message
	cancel     context.CancelFunc
	timeoutCtx context.Context
	added      time.Time
	sended     time.Time
}

func (c *ctx) hasCallback() bool {
	return c.completeC != nil
}

func (c *ctx) get() (meta.Message, error) {
	select {
	case rsp := <-c.completeC:
		c.close()
		return rsp, nil
	case <-c.timeoutCtx.Done():
		c.close()
		return nil, fmt.Errorf("time out with %d, added %s, sent %s", c.id, c.added, c.sended)
	}
}

func (c *ctx) close() {
	close(c.completeC)
	c.cancel()
}
