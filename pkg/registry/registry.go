package registry

// Registry is used for registry the taas proxy address to registry center
// So seata client can discovery the taas service from registry center
type Registry interface {
	Register(addr string) error
}
