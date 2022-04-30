package registry

import (
	"net/url"

	consulapi "github.com/hashicorp/consul/api"
	"seata.io/server/pkg/registry/consul"
)

func newConsulRegistry(u *url.URL) (Registry, error) {

	cfg := consulapi.DefaultConfig()
	cfg.Address = "http" + u.Host

	cli, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return consul.NewRegistry(cli)
}
