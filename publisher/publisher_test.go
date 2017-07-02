package publisher

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/stretchr/testify/assert"
)

type MockReader struct {
	records []eventsource.StreamRecord
}

func (m *MockReader) Read(ctx context.Context, startingOffset int64, recordCount int) ([]eventsource.StreamRecord, error) {
	if startingOffset >= int64(len(m.records)) {
		return nil, nil
	}

	records := m.records[startingOffset:]
	if v := len(records); recordCount > v {
		recordCount = v
	}
	records = records[0:recordCount]
	return records, nil
}

type MockHandler struct {
	events [][]byte
	ch     chan struct{}
}

func (m *MockHandler) Publish(ctx context.Context, event []byte) error {
	m.events = append(m.events, event)

	select {
	case m.ch <- struct{}{}:
	default:
	}
	return nil
}

var (
	id     = "abc"
	reader = &MockReader{
		records: []eventsource.StreamRecord{
			{
				AggregateID: id,
				Offset:      0,
				Record: eventsource.Record{
					Data:    []byte("a"),
					Version: 1,
				},
			},
			{
				AggregateID: id,
				Offset:      1,
				Record: eventsource.Record{
					Data:    []byte("b"),
					Version: 2,
				},
			},
			{
				AggregateID: id,
				Offset:      2,
				Record: eventsource.Record{
					Data:    []byte("c"),
					Version: 3,
				},
			},
		},
	}
)

func TestMakePollFunc(t *testing.T) {
	ctx := context.Background()
	cp := &MemoryCP{}

	t.Run("read partial batch", func(t *testing.T) {
		h := &MockHandler{}
		fn := makePollFunc(reader, h, "key", cp, 10, lookupDelay)
		offset := fn(ctx, 0)
		assert.Equal(t, int64(2), offset)
		assert.Len(t, h.events, 3)
		assert.Equal(t, h.events[0], reader.records[0].Data)
		assert.Equal(t, h.events[1], reader.records[1].Data)
		assert.Equal(t, h.events[2], reader.records[2].Data)
	})

	t.Run("read full batch", func(t *testing.T) {
		h := &MockHandler{}
		fn := makePollFunc(reader, h, "key", cp, 1, lookupDelay)

		assert.Equal(t, int64(0), fn(ctx, 0))
		assert.Len(t, h.events, 1)

		assert.Equal(t, int64(1), fn(ctx, 1))
		assert.Len(t, h.events, 2)

		assert.Equal(t, int64(2), fn(ctx, 2))
		assert.Len(t, h.events, 3)
	})

	t.Run("read nothing", func(t *testing.T) {
		h := &MockHandler{}
		fn := makePollFunc(reader, h, "key", cp, 10, lookupDelay)

		offset := int64(100)
		assert.Equal(t, offset, fn(ctx, offset))
		assert.Len(t, h.events, 0)
	})
}

func TestStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan struct{}, 3)
	defer close(ch)

	h := &MockHandler{ch: ch}
	key := "key"
	cp := &MemoryCP{}

	done, err := Start(ctx, reader, h, key, cp,
		WithListenerFunc(func(ctx context.Context) (<-chan struct{}, error) {
			ping := make(chan struct{}, 1)
			ping <- struct{}{}

			go func() {
				defer close(ping)
				<-ctx.Done()
			}()
			return ping, nil
		}),
	)
	assert.Nil(t, err)

	<-ch
	<-ch
	<-ch

	cancel()
	<-done

	assert.Len(t, h.events, 3)
}

func TestDefaultListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done, err := defaultListener(ctx)
	assert.Nil(t, err)

	cancel()
	<-done
}

func TestDelayFunc(t *testing.T) {
	assert.Equal(t, time.Second, lookupDelay(1))
	assert.Equal(t, time.Second*15, lookupDelay(25))
	assert.Equal(t, time.Minute, lookupDelay(100))
}

func TestRepeatUntilOk(t *testing.T) {
	ctx := context.Background()

	counter := 0
	delay := func(int) time.Duration { return time.Millisecond }
	repeatUntilOk(ctx, delay, func(ctx context.Context) error {
		counter++
		if counter == 3 {
			return nil
		}
		return io.ErrNoProgress
	})

	assert.Equal(t, 3, counter)
}

func TestWithBatchSize(t *testing.T) {
	batchSize := 25
	fn := WithBatchSize(batchSize)

	c := &config{}
	fn(c)

	assert.Equal(t, batchSize, c.batchSize)
}

func TestWithInterval(t *testing.T) {
	interval := time.Second
	fn := WithInterval(interval)

	c := &config{}
	fn(c)

	assert.Equal(t, interval, c.interval)
}
