package consul

import (
	"testing"

	consulapi "github.com/hashicorp/consul/api"
)

func TestRegistry(t *testing.T) {
	cfg := consulapi.DefaultConfig()

	cli, err := consulapi.NewClient(cfg)
	if err != nil {
		t.Fatalf("create Consul client failed with %+v", err)
	}

	reg, err := NewRegistry(cli)
	if err != nil {
		t.Fatalf("create Consul registry failed with error %+v", err)
		return
	}

	err = reg.Register("127.0.0.1:9093")
	if err != nil {
		t.Errorf("registry to Consul registry failed with error %+v", err)
		return
	}
}
