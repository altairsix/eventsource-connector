package publisher_test

import (
	"context"
	"testing"

	"github.com/altairsix/eventsource-connector/publisher"
	"github.com/stretchr/testify/assert"
)

func TestMemoryCP(t *testing.T) {
	cp := publisher.MemoryCP{}
	ctx := context.Background()
	key := "key"

	offset := int64(123)
	err := cp.Save(ctx, key, offset)
	assert.Nil(t, err)

	actual, err := cp.Load(ctx, key)
	assert.Nil(t, err)
	assert.Equal(t, offset, actual)
}
