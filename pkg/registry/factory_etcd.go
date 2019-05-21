package registry

import (
	"fmt"
	"net/url"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/util/format"
	"github.com/infinivision/taas/pkg/registry/etcd"
)

const (
	paramServers  = "servers"
	paramUsername = "user"
	paramPassword = "password"

	paramLease = "lease"
	paramGroup = "group"
)

func newEtcdRegistry(u *url.URL) (Registry, error) {
	cfg := clientv3.Config{}
	var servers []string
	servers = append(servers, fmt.Sprintf("http://%s", u.Host))
	if values, ok := u.Query()[paramServers]; ok {
		for _, value := range values {
			servers = append(servers, fmt.Sprintf("http://%s", value))
		}
	}
	cfg.Endpoints = servers

	user := u.Query().Get(paramUsername)
	if user != "" {
		cfg.Username = user
	}

	password := u.Query().Get(paramPassword)
	if password != "" {
		cfg.Password = password
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	var opts []etcd.Option
	lease := u.Query().Get(paramLease)
	if lease != "" {
		opts = append(opts, etcd.WithLease(format.MustParseStrInt64(lease)))
	}

	group := u.Query().Get(paramGroup)
	if group != "" {
		opts = append(opts, etcd.WithGroup(group))
	}

	return etcd.NewRegistry(client, opts...)
}
