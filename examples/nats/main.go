package main

import (
	"context"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource-publisher"
	"github.com/altairsix/eventsource-publisher/nats"
	go_nats "github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

type mock struct {
	records []eventsource.StreamRecord
}

func (m *mock) Read(ctx context.Context, offset int64, batchSize int) ([]eventsource.StreamRecord, error) {
	return nil, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// connect to nats
	st, _ := stan.Connect("test-cluster", "test", stan.NatsURL(go_nats.DefaultURL))

	// use the nats handler to publish events
	subject := "subject"
	h := nats.Handler(st, subject)

	// start the publisher
	done, _ := publisher.Start(ctx, reader, h, "key", publisher.MemoryCP{})

	// allow the publisher to poll for events
	time.Sleep(time.Second * 3)

	// stop the service
	cancel()
	<-done
}

var (
	id     = "abc"
	reader = &mock{
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
