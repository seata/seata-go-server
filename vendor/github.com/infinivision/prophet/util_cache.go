package prophet

import (
	"sync"
	"time"
)

type cacheItem struct {
	key    uint64
	value  interface{}
	expire time.Time
}

type resourceFreezeCache struct {
	sync.RWMutex

	items      map[uint64]cacheItem
	ttl        time.Duration
	gcInterval time.Duration
}

// newResourceFreezeCache returns a new expired resource freeze cache.
func newResourceFreezeCache(gcInterval time.Duration, ttl time.Duration) *resourceFreezeCache {
	c := &resourceFreezeCache{
		items:      make(map[uint64]cacheItem),
		ttl:        ttl,
		gcInterval: gcInterval,
	}

	return c
}

func (c *resourceFreezeCache) get(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expire.Before(time.Now()) {
		return nil, false
	}

	return item.value, true
}

func (c *resourceFreezeCache) set(key uint64, value interface{}) {
	c.setWithTTL(key, value, c.ttl)
}

func (c *resourceFreezeCache) setWithTTL(key uint64, value interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = cacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

func (c *resourceFreezeCache) delete(key uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

func (c *resourceFreezeCache) count() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *resourceFreezeCache) startGC() {
	go c.doGC()
}

func (c *resourceFreezeCache) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := 0
			now := time.Now()
			c.Lock()
			for key := range c.items {
				if value, ok := c.items[key]; ok {
					if value.expire.Before(now) {
						count++
						delete(c.items, key)
					}
				}
			}
			c.Unlock()
		}
	}
}
