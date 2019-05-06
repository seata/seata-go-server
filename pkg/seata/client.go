package seata

import (
	"sync"
	"sync/atomic"

	"github.com/fagongzi/goetty"
	"github.com/infinivision/taas/pkg/meta"
)

// Client seata
type Client struct {
	sync.RWMutex

	msgID   uint64
	op      uint64
	servers []string
	conns   *goetty.AddressBasedPool
}

// NewClient returns the seata client
func NewClient(servers ...string) *Client {
	return nil
}

// Send send msg to server
func (c *Client) Send(req meta.Message) (meta.Message, error) {
	msg := &meta.RPCMessage{}
	msg.Header.MsgType = req.Type()
	msg.Header.MsgID = atomic.AddUint64(&c.msgID, 1)
	msg.Header.Flag = meta.FlagRequest | meta.FlagSeataCodec
	if msg.Header.MsgType == meta.TypeHeartbeat {
		msg.Header.Flag |= meta.FlagHeartbeat
	}

	return nil, nil
}

func (c *Client) getConn() {

}

func (c *Client) next() string {
	return c.servers[atomic.AddUint64(&c.op, 1)&uint64(len(c.servers))]
}
