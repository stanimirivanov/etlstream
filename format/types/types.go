package types

import "io"

// Reader defines an interface for sequentially reading records of type T.
// It is designed to process large data sources without loading everything into memory.
type Reader[T any] interface {
	// Read reads and returns the next record from the underlying data source.
	// When the end of the data source is reached, it must return an io.EOF error.
	Read() (T, error)
}

// Writer defines an interface for sequentially writing records of type T.
type Writer[T any] interface {
	// Write writes a single record to the underlying data destination.
	Write(record T) error

	// Flush ensures that any buffered data is written to the underlying destination.
	// This is crucial when dealing with buffered I/O to prevent data loss.
	Flush() error

	// Close signals that no more records will be written, allowing
	// serializers to write footers (like ']') and close underlying files.
	io.Closer
}

// Serializer acts as a factory interface for creating typed Readers and Writers.
// It bridges the gap between raw byte streams (io.Reader/io.Writer) and strongly-typed records.
type Serializer[T any] interface {
	// CreateReader wraps an io.Reader and returns a Reader[T] to parse records.
	CreateReader(r io.Reader) (Reader[T], error)

	// CreateWriter wraps an io.Writer and returns a Writer[T] to format records.
	CreateWriter(w io.Writer) (Writer[T], error)
}

// Comparator defines a function type for comparing two instances of type T.
// It returns:
//   - A negative integer if a < b
//   - Zero if a == b
//   - A positive integer if a > b
//
// This signature matches the requirement for Go's standard slices.SortFunc.
type Comparator[T any] func(a, b T) int
