package bigsorter

import "io"

// Serializer acts as a factory interface for creating typed Readers and Writers.
// It bridges the gap between raw byte streams (io.Reader/io.Writer) and strongly-typed records.
type Serializer[T any] interface {
	// CreateReader wraps an io.Reader and returns a Reader[T] to parse records.
	CreateReader(r io.Reader) (Reader[T], error)

	// CreateWriter wraps an io.Writer and returns a Writer[T] to format records.
	CreateWriter(w io.Writer) (Writer[T], error)
}
