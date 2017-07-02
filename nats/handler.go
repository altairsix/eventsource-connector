package nats

import (
	"context"

	"github.com/altairsix/eventsource-publisher"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

// Handler creates a handler that publishes to nats streaming
func Handler(st stan.Conn, subject string) publisher.Handler {
	return publisher.HandlerFunc(func(ctx context.Context, data []byte) error {
		return st.Publish(subject, data)
	})
}

// Listener constructs a NATS listener that listens on a specific subject for update requests
func Listener(nc *nats.Conn, subject string) publisher.ListenerFunc {
	return func(ctx context.Context) (<-chan struct{}, error) {
		ping := make(chan struct{}, 1)

		sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
			ping <- struct{}{}
		})
		if err != nil {
			close(ping)
			return nil, err
		}

		go func() {
			defer close(ping)
			<-ctx.Done()
			sub.Unsubscribe()
		}()

		return ping, nil
	}
}
