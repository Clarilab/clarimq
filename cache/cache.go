package cache

import (
	"sync"

	"github.com/Clarilab/clarimq"
)

// BasicInMemoryCache is a basic in-memory cache implementation of the clarimq.PublishingCache interface.
type BasicInMemoryCache struct {
	storeMU sync.Mutex
	store   map[string]clarimq.Publishing
}

// NewBasicMemoryCache creates a new in-memory cache.
func NewBasicMemoryCache() *BasicInMemoryCache {
	return &BasicInMemoryCache{
		store: make(map[string]clarimq.Publishing),
	}
}

// Put implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) Put(pbl clarimq.Publishing) error {
	c.storeMU.Lock()
	c.store[pbl.ID()] = pbl
	c.storeMU.Unlock()

	return nil
}

// PopAll implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) PopAll() ([]clarimq.Publishing, error) {
	data := make([]clarimq.Publishing, 0, len(c.store))

	c.storeMU.Lock()
	for key, val := range c.store {
		data = append(data, val)

		delete(c.store, key)
	}

	c.storeMU.Unlock()

	return data, nil
}

// Len implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) Len() int {
	c.storeMU.Lock()
	defer c.storeMU.Unlock()

	return len(c.store)
}

// Flush implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) Flush() error {
	c.storeMU.Lock()
	for key := range c.store {
		delete(c.store, key)
	}

	c.storeMU.Unlock()

	return nil
}
