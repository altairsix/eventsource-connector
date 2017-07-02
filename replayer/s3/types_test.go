package s3_test

import (
	"testing"

	"github.com/altairsix/eventsource-connector/replayer"
	"github.com/altairsix/eventsource-connector/replayer/s3"
	"github.com/stretchr/testify/assert"
)

func TestImplementsReadCloser(t *testing.T) {
	var r interface{} = &s3.Replayer{}
	_, ok := r.(replayer.ReadCloser)
	assert.True(t, ok)
}
