package prophet

// Selector is an interface to select source and target container to schedule.
type Selector interface {
	SelectSource(containers []*ContainerRuntime, filters ...Filter) *ContainerRuntime
	SelectTarget(containers []*ContainerRuntime, filters ...Filter) *ContainerRuntime
}

type balanceSelector struct {
	kind    ResourceKind
	filters []Filter
}

func newBalanceSelector(kind ResourceKind, filters []Filter) *balanceSelector {
	return &balanceSelector{
		kind:    kind,
		filters: filters,
	}
}

func (s *balanceSelector) SelectSource(containers []*ContainerRuntime, filters ...Filter) *ContainerRuntime {
	filters = append(filters, s.filters...)

	var result *ContainerRuntime
	for _, container := range containers {
		if filterSource(container, filters) {
			continue
		}
		if result == nil || result.ResourceScore(s.kind) < container.ResourceScore(s.kind) {
			result = container
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(containers []*ContainerRuntime, filters ...Filter) *ContainerRuntime {
	filters = append(filters, s.filters...)

	var result *ContainerRuntime
	for _, container := range containers {
		if filterTarget(container, filters) {
			continue
		}
		if result == nil || result.ResourceScore(s.kind) > container.ResourceScore(s.kind) {
			result = container
		}
	}
	return result
}
