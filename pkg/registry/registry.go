package registry

// Registry is used for registry the seata proxy address to registry center
// So seata client can discovery the seata service from registry center
type Registry interface {
	Register(addr string) error
}
