package bigsorter

// Comparator defines a function type for comparing two instances of type T.
// It returns:
//   - A negative integer if a < b
//   - Zero if a == b
//   - A positive integer if a > b
//
// This signature matches the requirement for Go's standard slices.SortFunc.
type Comparator[T any] func(a, b T) int
