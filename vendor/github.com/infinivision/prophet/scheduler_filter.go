package prophet

// Filter is used for filter container
type Filter interface {
	// FilterSource returns true means skip this container for schedule
	FilterSource(container *ContainerRuntime) bool
	// FilterTarget returns true means skip this container for schedule
	FilterTarget(container *ContainerRuntime) bool
}

func filterSource(container *ContainerRuntime, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(container) {
			return true
		}
	}
	return false
}

func filterTarget(container *ContainerRuntime, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(container) {
			return true
		}
	}
	return false
}

type stateFilter struct {
	cfg *Cfg
}

type storageThresholdFilter struct {
	cfg *Cfg
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

type healthFilter struct {
	cfg *Cfg
}

type cacheFilter struct {
	freezeCache *resourceFreezeCache
}

type distinctScoreFilter struct {
	cfg        *Cfg
	containers []*ContainerRuntime
	safeScore  float64
}

type snapshotCountFilter struct {
	cfg *Cfg
}

// NewStorageThresholdFilter returns a filter for choose resource container by storage rate
func NewStorageThresholdFilter(cfg *Cfg) Filter {
	return &storageThresholdFilter{cfg: cfg}
}

// NewStateFilter returns a filter for choose resource container by state
func NewStateFilter(cfg *Cfg) Filter {
	return &stateFilter{cfg: cfg}
}

// NewExcludedFilter returns a filter for choose resource container by excluded value
func NewExcludedFilter(sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

// NewHealthFilter returns a filter for choose resource container by health info
func NewHealthFilter(cfg *Cfg) Filter {
	return &healthFilter{cfg: cfg}
}

// NewCacheFilter returns a filter for choose resource container by runtime cache
func NewCacheFilter(freezeCache *resourceFreezeCache) Filter {
	return &cacheFilter{freezeCache: freezeCache}
}

// NewBlockFilter returns a filter for choose resource container by block
func NewBlockFilter() Filter {
	return &blockFilter{}
}

// NewDistinctScoreFilter a filter for ensures that distinct score will not decrease.
func NewDistinctScoreFilter(cfg *Cfg, containers []*ContainerRuntime, source *ContainerRuntime) Filter {
	newContainers := make([]*ContainerRuntime, 0, len(containers)-1)
	for _, s := range newContainers {
		if s.meta.ID() == source.meta.ID() {
			continue
		}
		newContainers = append(newContainers, s)
	}

	return &distinctScoreFilter{
		cfg:        cfg,
		containers: newContainers,
		safeScore:  cfg.getDistinctScore(newContainers, source),
	}
}

// NewSnapshotCountFilter returns snapshot filter
func NewSnapshotCountFilter(cfg *Cfg) Filter {
	return &snapshotCountFilter{cfg: cfg}
}

func (f *stateFilter) filter(container *ContainerRuntime) bool {
	return !(container.IsUp() && container.Downtime() < f.cfg.MaxAllowContainerDownDuration)
}

func (f *stateFilter) FilterSource(container *ContainerRuntime) bool {
	return f.filter(container)
}

func (f *stateFilter) FilterTarget(container *ContainerRuntime) bool {
	return f.filter(container)
}

func (f *storageThresholdFilter) FilterSource(container *ContainerRuntime) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(container *ContainerRuntime) bool {
	return container.StorageUsedRatio() > f.cfg.MinAvailableStorageUsedRate
}

func (f *excludedFilter) FilterSource(container *ContainerRuntime) bool {
	_, ok := f.sources[container.meta.ID()]
	return ok
}

func (f *excludedFilter) FilterTarget(container *ContainerRuntime) bool {
	_, ok := f.targets[container.meta.ID()]
	return ok
}

type blockFilter struct{}

func (f *blockFilter) FilterSource(container *ContainerRuntime) bool {
	return container.IsBlocked()
}

func (f *blockFilter) FilterTarget(container *ContainerRuntime) bool {
	return container.IsBlocked()
}

func (f *healthFilter) filter(container *ContainerRuntime) bool {
	if container.HasNoneHeartbeat() || container.busy {
		return true
	}

	return container.Downtime() > f.cfg.MaxAllowContainerDownDuration
}

func (f *healthFilter) FilterSource(container *ContainerRuntime) bool {
	return f.filter(container)
}

func (f *healthFilter) FilterTarget(container *ContainerRuntime) bool {
	return f.filter(container)
}

func (f *cacheFilter) FilterSource(container *ContainerRuntime) bool {
	_, ok := f.freezeCache.get(container.meta.ID())
	return ok
}

func (f *cacheFilter) FilterTarget(container *ContainerRuntime) bool {
	return false
}

func (f *distinctScoreFilter) FilterSource(container *ContainerRuntime) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(container *ContainerRuntime) bool {
	return f.cfg.getDistinctScore(f.containers, container) < f.safeScore
}

func (f *snapshotCountFilter) filter(container *ContainerRuntime) bool {
	return container.sendingSnapCount > f.cfg.MaxLimitSnapshotsCount ||
		container.receivingSnapCount > f.cfg.MaxLimitSnapshotsCount ||
		container.applyingSnapCount > f.cfg.MaxLimitSnapshotsCount
}

func (f *snapshotCountFilter) FilterSource(container *ContainerRuntime) bool {
	return f.filter(container)
}

func (f *snapshotCountFilter) FilterTarget(container *ContainerRuntime) bool {
	return f.filter(container)
}
