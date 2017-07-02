package publisher

import (
	"context"
	"time"

	"github.com/altairsix/eventsource"
)

const (
	// DefaultBatchSize specifies the default number of records to read from a stream
	DefaultBatchSize = 100
)

// Handler represents the target where events will be delivered
type Handler interface {
	// Publish is invoked to deliver a []byte encoded event
	Publish(ctx context.Context, data []byte) error
}

// HandlerFunc provides a function type helper to the publisher
type HandlerFunc func(ctx context.Context, data []byte) error

// Publish implements the Publisher interface
func (fn HandlerFunc) Publish(ctx context.Context, data []byte) error {
	return fn(ctx, data)
}

// Listener represents an external trigger that can cause a poll to occur
// immediately rather than waiting for the interval
type Listener interface {
	// New returns a new chan that describes the listener
	New(ctx context.Context) (<-chan struct{}, error)
}

// ListenerFunc provides a func helper to Listener
type ListenerFunc func(ctx context.Context) (<-chan struct{}, error)

// New implements the Listener.New
func (fn ListenerFunc) New(ctx context.Context) (<-chan struct{}, error) {
	return fn(ctx)
}

type config struct {
	interval  time.Duration
	batchSize int
	delayFunc func(attempts int) time.Duration
	listener  Listener
}

// StartOption provides options to the Start function
type StartOption func(*config)

// WithInterval specifies the polling interval; defaults to 15s
func WithInterval(interval time.Duration) StartOption {
	return func(c *config) {
		c.interval = interval
	}
}

// WithBatchSize specifies the number of records to read from the stream reader at a time
func WithBatchSize(batchSize int) StartOption {
	return func(c *config) {
		c.batchSize = batchSize
	}
}

// WithListener allows for an optional listener to request the publisher poll immediately
// rather than waiting for the interval
func WithListener(l Listener) StartOption {
	return func(c *config) {
		if l != nil {
			c.listener = l
		}
	}
}

// WithListenerFunc provides another way to introduce a listener
func WithListenerFunc(fn ListenerFunc) StartOption {
	return func(c *config) {
		if fn != nil {
			c.listener = fn
		}
	}
}

func defaultListener(ctx context.Context) (<-chan struct{}, error) {
	ping := make(chan struct{})

	go func() {
		defer close(ping)
		<-ctx.Done()
	}()

	return ping, nil
}

func makePollFunc(r eventsource.StreamReader, h Handler, key string, cp Checkpointer, batchSize int, delayFunc func(int) time.Duration) func(context.Context, int64) int64 {
	return func(ctx context.Context, offset int64) int64 {
		records, err := r.Read(ctx, offset, batchSize)
		if err != nil {
			return offset
		}

		newOffset := offset
		for _, record := range records {
			repeatUntilOk(ctx, delayFunc, func(ctx context.Context) error {
				return h.Publish(ctx, record.Data)
			})
			newOffset = record.Offset
		}

		if offset == newOffset {
			return offset
		}

		repeatUntilOk(ctx, delayFunc, func(ctx context.Context) error {
			return cp.Save(ctx, key, newOffset)
		})

		return newOffset
	}
}

// Start the publisher
func Start(
	ctx context.Context, // context is for cancellation
	r eventsource.StreamReader, // where our events come from
	h Handler, // where our events go
	key string,
	cp Checkpointer, // allows us to save checkpoints
	opts ...StartOption, // options
) (<-chan struct{}, error) {
	c := &config{
		interval:  time.Second * 15,
		listener:  ListenerFunc(defaultListener),
		batchSize: DefaultBatchSize,
		delayFunc: lookupDelay,
	}

	for _, opt := range opts {
		opt(c)
	}

	offset, err := cp.Load(ctx, key)
	if err != nil {
		return nil, err
	}

	pollFunc := makePollFunc(r, h, key, cp, c.batchSize, c.delayFunc)

	ping, err := c.listener.New(ctx)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		timer := time.NewTicker(c.interval)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ping:
				// check to see if context was canceled; prevents race condition between ping closing and this
				// goroutine's closing
				select {
				case <-ctx.Done():
					return
				default:
				}

				offset = pollFunc(ctx, offset)

			case <-timer.C:
				offset = pollFunc(ctx, offset)
			}
		}
	}()

	return done, nil
}

func repeatUntilOk(ctx context.Context, delayFunc func(int) time.Duration, fn func(ctx context.Context) error) {
	attempts := 0

	for {
		attempts++

		if err := fn(ctx); err == nil {
			return
		}

		delay := delayFunc(attempts)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}
}

func lookupDelay(attempts int) time.Duration {
	d := time.Second
	if attempts > 50 {
		d = time.Minute
	} else if attempts > 10 {
		d = time.Second * 15
	}
	return d
}
