package jsonarray

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/stanimirivanov/etlstream/extsort"
)

// Serializer implements etlstream.Serializer for JSON arrays containing type T.
type Serializer[T any] struct{}

func (s Serializer[T]) CreateReader(r io.Reader) (extsort.Reader[T], error) {
	dec := json.NewDecoder(r)

	// Expect the opening bracket of the JSON array
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array opening '[', got %v", t)
	}

	return &reader[T]{dec: dec}, nil
}

func (s Serializer[T]) CreateWriter(w io.Writer) (extsort.Writer[T], error) {
	// Start the JSON array
	if _, err := w.Write([]byte("[\n")); err != nil {
		return nil, err
	}
	return &writer[T]{w: w, enc: json.NewEncoder(w), first: true}, nil
}

type reader[T any] struct {
	dec *json.Decoder
}

func (r *reader[T]) Read() (T, error) {
	var zero T
	if !r.dec.More() {
		// Consume the closing bracket
		if _, err := r.dec.Token(); err != nil {
			return zero, err
		}
		return zero, io.EOF
	}

	var val T
	err := r.dec.Decode(&val)
	return val, err
}

type writer[T any] struct {
	w     io.Writer
	enc   *json.Encoder
	first bool
}

func (w *writer[T]) Write(record T) error {
	if !w.first {
		if _, err := w.w.Write([]byte(",")); err != nil {
			return err
		}
	}
	w.first = false
	return w.enc.Encode(record)
}

func (w *writer[T]) Flush() error {
	return nil // encoding/json writes directly to the underlying writer
}

func (w *writer[T]) Close() error {
	_, err := w.w.Write([]byte("]\n"))
	return err
}
