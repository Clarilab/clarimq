package cache

import (
	"github.com/Clarilab/clarimq"
)

// BasicInMemoryCache is a basic in-memory cache implementation of the clarimq.PublishingCache interface.
type BasicInMemoryCache struct {
	store map[string]clarimq.Publishing
}

// NewBasicMemoryCache creates a new in-memory cache.
func NewBasicMemoryCache() *BasicInMemoryCache {
	return &BasicInMemoryCache{
		store: make(map[string]clarimq.Publishing),
	}
}

// Put implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) Put(pbl clarimq.Publishing) error {
	c.store[pbl.ID()] = pbl

	return nil
}

// PopAll implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) PopAll() ([]clarimq.Publishing, error) {
	data := make([]clarimq.Publishing, 0, len(c.store))

	for key, val := range c.store {
		data = append(data, val)

		delete(c.store, key)
	}

	return data, nil
}

// Len implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) Len() int {
	return len(c.store)
}

// Flush implements the clarimq.PublishingCache interface.
func (c *BasicInMemoryCache) Flush() error {
	for key := range c.store {
		delete(c.store, key)
	}

	return nil
}
