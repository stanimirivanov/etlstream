package generator

import (
	"github.com/stanimirivanov/etlstream/format/types"
)

// Reader wraps an underlying types.Reader and pre-fetches records into a
// buffered channel, so that the consumer (the Min-Heap, and therefore the
// k-way merger) can drain the channel without blocking on synchronous I/O
// from the underlying stream.
type Reader[T any] struct {
	r   types.Reader[T]
	ch  chan T
	err error
}

// DefaultBufferSize is the number of records each generator buffers ahead
// of the consumer. It is small enough to keep memory bounded while large
// enough to amortise disk latency.
const DefaultBufferSize = 512

// NewReader wraps r with a generator that feeds records into the buffered
// channel. A background goroutine reads from r and sends into ch, so the
// merge loop only ever reads from memory once the buffer is drained.
func NewReader[T any](r types.Reader[T]) *Reader[T] {
	p := &Reader[T]{r: r, ch: make(chan T, DefaultBufferSize)}
	go p.feed()
	return p
}

func (p *Reader[T]) feed() {
	defer close(p.ch)
	for {
		rec, err := p.r.Read()
		if err != nil {
			p.err = err
			return
		}
		p.ch <- rec
	}
}

// Read returns the next pre-fetched record, or the EOF/error from the
// underlying reader once the stream is exhausted.
func (p *Reader[T]) Read() (T, error) {
	var zero T
	rec, ok := <-p.ch
	if !ok {
		return zero, p.err
	}
	return rec, nil
}
