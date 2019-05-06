package prophet

func (rc *Runtime) handleContainer(source *ContainerRuntime) {
	rc.Lock()
	defer rc.Unlock()

	evt := EventContainerChanged
	if _, ok := rc.containers[source.meta.ID()]; !ok {
		evt = EventContainerCreated
	}

	rc.containers[source.meta.ID()] = source
	rc.p.notifyEvent(newContainerEvent(evt, source.meta))
}

func (rc *Runtime) handleResource(source *ResourceRuntime) error {
	rc.Lock()
	defer rc.Unlock()

	current := rc.getResourceWithoutLock(source.meta.ID())
	if current == nil {
		err := rc.doPutResource(source)
		if err != nil {
			return err
		}
		rc.p.notifyEvent(newResourceEvent(EventResourceCreated, source.meta))
		if source.leaderPeer != nil {
			rc.p.notifyEvent(newLeaderChangerEvent(source.meta.ID(), source.leaderPeer.ID))
		}
		return nil
	}

	// resource meta is stale, return an error.
	if current.meta.Stale(source.meta) {
		return errStaleResource
	}

	// resource meta is updated, update kv and cache.
	if current.meta.Changed(source.meta) {
		err := rc.doPutResource(source)
		if err != nil {
			return err
		}

		rc.p.notifyEvent(newResourceEvent(EventResourceChaned, source.meta))
		return nil
	}

	if current.leaderPeer != nil &&
		current.leaderPeer.ID != source.leaderPeer.ID {
		log.Infof("prophet: resource %d leader changed, from %d to %d",
			current.meta.ID(),
			current.leaderPeer.ID,
			source.leaderPeer.ID)
		rc.p.notifyEvent(newLeaderChangerEvent(source.meta.ID(), source.leaderPeer.ID))
	}

	// resource meta is the same, update cache only.
	rc.putResourceInCache(source)
	return nil
}

func (rc *Runtime) doPutResource(source *ResourceRuntime) error {
	err := rc.p.store.PutResource(source.meta)
	if err != nil {
		return err
	}

	rc.putResourceInCache(source)
	return nil
}

func (rc *Runtime) putResourceInCache(origin *ResourceRuntime) {
	if origin, ok := rc.resources[origin.meta.ID()]; ok {
		rc.removeResource(origin)
	}

	rc.resources[origin.meta.ID()] = origin

	if origin.leaderPeer == nil || origin.leaderPeer.ID == 0 {
		return
	}

	// Add to leaders and followers.
	for _, peer := range origin.meta.Peers() {
		containerID := peer.ContainerID
		if peer.ID == origin.leaderPeer.ID {
			// Add leader peer to leaders.
			container, ok := rc.leaders[containerID]
			if !ok {
				container = make(map[uint64]*ResourceRuntime)
				rc.leaders[containerID] = container
			}
			container[origin.meta.ID()] = origin
		} else {
			// Add follower peer to followers.
			container, ok := rc.followers[containerID]
			if !ok {
				container = make(map[uint64]*ResourceRuntime)
				rc.followers[containerID] = container
			}
			container[origin.meta.ID()] = origin
		}
	}
}

func (rc *Runtime) removeResource(origin *ResourceRuntime) {
	delete(rc.resources, origin.meta.ID())

	// Remove from leaders and followers.
	for _, peer := range origin.meta.Peers() {
		delete(rc.leaders[peer.ContainerID], origin.meta.ID())
		delete(rc.followers[peer.ContainerID], origin.meta.ID())
	}
}
