package jsonlines

import (
	"bufio"
	"encoding/json"
	"io"

	"github.com/stanimirivanov/etlstream/extsort"
)

// Serializer implements etlstream.Serializer for JSON Lines (NDJSON).
type Serializer[T any] struct {
	// MaxCapacity defines the maximum size in bytes for a single JSON line.
	// If zero, the bufio.Scanner default (64KB) is used.
	MaxCapacity int
}

func (s Serializer[T]) CreateReader(r io.Reader) (extsort.Reader[T], error) {
	scanner := bufio.NewScanner(r)
	if s.MaxCapacity > 0 {
		// Initialize with a standard 64KB buffer, up to the defined max capacity
		scanner.Buffer(make([]byte, 64*1024), s.MaxCapacity)
	}
	return &reader[T]{scanner: scanner}, nil
}

func (s Serializer[T]) CreateWriter(w io.Writer) (extsort.Writer[T], error) {
	return &writer[T]{enc: json.NewEncoder(w)}, nil
}

type reader[T any] struct {
	scanner *bufio.Scanner
}

func (r *reader[T]) Read() (T, error) {
	var zero T
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return zero, err
		}
		return zero, io.EOF
	}

	var val T
	if err := json.Unmarshal(r.scanner.Bytes(), &val); err != nil {
		return zero, err
	}
	return val, nil
}

type writer[T any] struct {
	enc *json.Encoder
}

func (w *writer[T]) Write(record T) error {
	// json.Encoder automatically appends a newline character
	return w.enc.Encode(record)
}

func (w *writer[T]) Flush() error { return nil }
func (w *writer[T]) Close() error { return nil }
