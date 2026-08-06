package bigsorter

// Reader defines an interface for sequentially reading records of type T.
// It is designed to process large data sources without loading everything into memory.
type Reader[T any] interface {
	// Read reads and returns the next record from the underlying data source.
	// When the end of the data source is reached, it must return an io.EOF error.
	Read() (T, error)
}
