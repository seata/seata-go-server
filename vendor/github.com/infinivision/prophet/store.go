package prophet

// Store meta store
type Store interface {
	// CampaignLeader is for leader election
	// if we are win the leader election, the enableLeaderFun will call
	CampaignLeader(ttl int64, enableLeaderFun, disableLeaderFun func()) error
	// ResignLeader delete leader itself and let others start a new election again.
	ResignLeader() error
	// GetCurrentLeader return current leader
	GetCurrentLeader() (*Node, error)
	// WatchLeader watch leader,
	// this funcation will return unitl the leader's lease is timeout
	// or server closed
	WatchLeader()

	// PutResource puts the meta to the store
	PutResource(meta Resource) error
	// PutContainer puts the meta to the store
	PutContainer(meta Container) error
	// GetContainer returns the spec container
	GetContainer(id uint64) (Container, error)
	// LoadResources load all resources
	LoadResources(limit int64, do func(Resource)) error
	// LoadContainers load all containers
	LoadContainers(limit int64, do func(Container)) error

	// AllocID returns the alloc id
	AllocID() (uint64, error)

	// PutBootstrapped put cluster is bootstrapped
	PutBootstrapped(container Container, res Resource) (bool, error)
}
