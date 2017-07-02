package nats_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/altairsix/eventsource-publisher/nats"
	go_nats "github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/stretchr/testify/assert"
)

func TestListener(t *testing.T) {
	nc, err := go_nats.Connect(go_nats.DefaultURL)
	assert.Nil(t, err)
	defer nc.Close()

	subject := strconv.FormatInt(time.Now().UnixNano(), 36)
	listener := nats.Listener(nc, subject)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	ping, err := listener.New(ctx)
	assert.Nil(t, err)

	err = nc.Publish(subject, []byte(nil))
	assert.Nil(t, err)

	<-ping

	cancel()
	_, ok := <-ping
	assert.False(t, ok)
}

func TestHandler(t *testing.T) {
	st, err := stan.Connect("test-cluster", "client")
	assert.Nil(t, err)

	content := "hello world"
	subject := strconv.FormatInt(time.Now().UnixNano(), 36)
	h := nats.Handler(st, subject)
	err = h.Publish(context.Background(), []byte(content))
	assert.Nil(t, err)

	received := make(chan string, 8)
	fn := func(msg *stan.Msg) {
		select {
		case received <- string(msg.Data):
		default:
		}
	}
	sub, err := st.Subscribe(subject, fn, stan.StartWithLastReceived())
	assert.Nil(t, err)

	assert.Equal(t, content, <-received)
	sub.Unsubscribe()
}
