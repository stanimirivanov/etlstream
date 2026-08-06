package bigsorter

// Writer defines an interface for sequentially writing records of type T.
type Writer[T any] interface {
	// Write writes a single record to the underlying data destination.
	Write(record T) error

	// Flush ensures that any buffered data is written to the underlying destination.
	// This is crucial when dealing with buffered I/O to prevent data loss.
	Flush() error
}
