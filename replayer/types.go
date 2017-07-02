package replayer

import "io"

// ReadCloser reads []byte encoded events from a replay stream until it reaches the end.
type ReadCloser interface {
	io.Closer

	// ReadEvent reads a single []byte encoded event from the replay stream.  Returns io.EOF
	// once the end of the stream is reached
	ReadEvent() ([]byte, error)
}
