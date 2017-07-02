package publisher

import (
	"context"
)

// Checkpointer saves offsets to persistent data store
type Checkpointer interface {
	// Save the offset for the specified key to the data store
	Save(ctx context.Context, key string, offset int64) error

	// Load the offset for the specified key from the data store
	Load(ctx context.Context, key string) (int64, error)
}

// MemoryCP provides an in memory check pointer; useful for testing
type MemoryCP map[string]int64

// Save the offset for the specified key to the data store
func (m MemoryCP) Save(ctx context.Context, key string, offset int64) error {
	m[key] = offset
	return nil
}

// Load the offset for the specified key from the data store
func (m MemoryCP) Load(ctx context.Context, key string) (int64, error) {
	return m[key], nil
}
