package prophet

// shouldBalance returns true if we should balance the source and target container.
// The min balance diff provides a buffer to make the cluster stable, so that we
// don't need to schedule very frequently.
func shouldBalance(source, target *ContainerRuntime, kind ResourceKind) bool {
	sourceCount := source.ResourceCount(kind)
	sourceContainer := source.ResourceScore(kind)
	targetContainer := target.ResourceScore(kind)

	if targetContainer >= sourceContainer {
		return false
	}

	diffRatio := 1 - targetContainer/sourceContainer
	diffCount := diffRatio * float64(sourceCount)

	return diffCount >= minBalanceDiff(sourceCount)
}

func adjustBalanceLimit(rt *Runtime, kind ResourceKind) uint64 {
	containers := rt.Containers()

	counts := make([]float64, 0, len(containers))
	for _, c := range containers {
		counts = append(counts, float64(c.ResourceCount(kind)))
	}

	limit, _ := StandardDeviation(Float64Data(counts))
	return maxUint64(1, uint64(limit))
}
