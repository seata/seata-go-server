package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

func TestSend(t *testing.T) {
	resourceErr := false
	rpcErr := false
	resourceAddrDetecter := func(uint64, string) (meta.ResourceManager, error) {
		if resourceErr {
			return meta.ResourceManager{}, errors.New("just for test")
		}

		return meta.ResourceManager{
			ProxySID: "test-pid",
		}, nil
	}

	rpcFunc := func(meta.ResourceManager, meta.Notify) error {
		if rpcErr {
			return errors.New("just for test")
		}

		return nil
	}

	tran := NewTransport(1, resourceAddrDetecter, rpcFunc).(*transport)
	err := tran.Start()
	assert.Nil(t, err, "Start transport failed")
	assert.Equal(t, 1, len(tran.sendC), "check send channel count failed")

	completeC := make(chan struct{}, 1)
	cb := func(meta.Notify) {
		completeC <- struct{}{}
	}
	tran.AsyncSend("test-resource", meta.Notify{}, cb)
	select {
	case <-completeC:
		break
	case <-time.After(time.Second):
		assert.Fail(t, "sent failed, timeout")
		break
	}

	resourceErr = true
	tran.AsyncSend("test-resource", meta.Notify{}, cb)
	select {
	case <-completeC:
		assert.Fail(t, "check resourceAddrDetecter failed")
		break
	case <-time.After(time.Second):
		break
	}
	resourceErr = false
	select {
	case <-completeC:
		break
	case <-time.After(time.Second * 2):
		assert.Fail(t, "sent failed, retry timeout")
		break
	}

	resourceErr = false
	rpcErr = true
	tran.AsyncSend("test-resource", meta.Notify{}, cb)
	select {
	case <-completeC:
		assert.Fail(t, "check rpc failed")
		break
	case <-time.After(time.Second):
		break
	}
	rpcErr = false
	select {
	case <-completeC:
		break
	case <-time.After(time.Second * 2):
		assert.Fail(t, "sent failed, retry timeout")
		break
	}
}
