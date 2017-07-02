package s3

import (
	"errors"

	"github.com/altairsix/eventsource"
	"github.com/aws/aws-sdk-go/service/s3"
)

// config holds configuration options
type config struct {
	filter func(filename string) bool
	api    *s3.S3
}

// Option allows the behavior of the replay.ReadCloser to be specified
type Option func(c *config)

// WithFilter allows an optional filename filter to be specified; by
// default,
func WithFilter(filter func(filename string) bool) Option {
	return func(c *config) {
		c.filter = filter
	}
}

// WithS3 allows an instance of an S3 client to be provided; by default
// New should construct a default s3 client from environment variables
func WithS3(api *s3.S3) Option {
	return func(c *config) {
		c.api = api
	}
}

// Replayer replays events from an S3 stream; you may assume events are
// encoded 1 per line as a base64 encoded string.
//
// * files with a .gz extension should be ungzipped
// * files with a .zip extension should be unziped
type Replayer struct {
	api *s3.S3
}

// Close releases any resources associated with the stream replayer
func (r *Replayer) Close() error {
	return errors.New("implement me")
}

// ReadEvent reads a single event from the s3 stream
func (r *Replayer) ReadEvent() ([]byte, error) {
	return nil, errors.New("implement me")
}

// New constructs a new s3.Replayer which should implement replay.ReadCloser
// Should construct a default s3 client from environment variables.
//
// s3uri will be of the form s3://bucket/path
//
func New(s3uri string, opts ...Option) *Replayer {
	eventsource.StreamReader
	c := &config{
		filter: func(filename string) bool { return true },
	}

	for _, opt := range opts {
		opt(c)
	}

	panic("implement me")
}
