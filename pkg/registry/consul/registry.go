package consul

import (
	"fmt"
	"net"
	"strconv"

	consulapi "github.com/hashicorp/consul/api"
)

var (
	registryKeyPrefix = "registry-seata-"
)

// Registry consul registry
type Registry struct {
	cli *consulapi.Client
}

// NewRegistry returns a new consul registry
func NewRegistry(cli *consulapi.Client) (*Registry, error) {
	reg := new(Registry)

	reg.cli = cli

	return reg, nil
}

// Register registers the address to Consul
func (r *Registry) Register(addr string) error {
	svc, err := r.service(addr)
	if err != nil {
		return err
	}

	r.cli.Agent().ServiceRegister(svc)

	return nil
}

func (r *Registry) service(addr string) (*consulapi.AgentServiceRegistration, error) {

	host, portstr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(portstr)
	if err != nil {
		return nil, err
	}

	svc := new(consulapi.AgentServiceRegistration)
	svc.Address = host
	svc.Port = port
	svc.Name = r.registryKey(addr)
	svc.Tags = []string{"seata"}
	// TODO Is svc.ID needed?

	svc.Check = r.healthCheck(svc)

	return svc, nil
}

func (r *Registry) healthCheck(svc *consulapi.AgentServiceRegistration) *consulapi.AgentServiceCheck {
	c := new(consulapi.AgentServiceCheck)
	c.HTTP = fmt.Sprintf("http://%s:%d/check", svc.Address, svc.Port)
	c.Timeout = "5s"
	c.Interval = "5s"
	return c
}

func (r *Registry) registryKey(addr string) string {
	return fmt.Sprintf("%s-%s", registryKeyPrefix, addr)
}
