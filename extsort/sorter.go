package extsort

import (
	"context"
	"io"
)

// Sorter configures and executes the external merge sort process.
type Sorter[T any] struct {
	Serializer Serializer[T]
	Comparator Comparator[T]

	// MaxItems limits the number of records held in memory per chunk.
	// If set to <= 0, it defaults to 100,000.
	MaxItems int

	// TempDir specifies where temporary chunk files should be stored.
	// If empty, the system's default temporary directory is used.
	TempDir string

	// Concurrency specifies the number of parallel workers used during the split phase.
	// If set to <= 0, it defaults to runtime.NumCPU().
	Concurrency int

	// ProgressFunc is called periodically with metrics if provided.
	ProgressFunc ProgressFunc
}

// Sort reads records from the input, sorts them, and writes to the output.
// It uses context.Background() by default.
func (s *Sorter[T]) Sort(input io.Reader, output io.Writer) error {
	return s.SortContext(context.Background(), input, output)
}

// SortContext performs the external sort but listens for cancellation signals.
func (s *Sorter[T]) SortContext(ctx context.Context, input io.Reader, output io.Writer) error {
	// Phase 1: Split
	// We will soon update splitter.Split to accept ctx and a progress callback
	/*
		tempFiles, err := splitter.Split(ctx, input, splitter.Options[T]{ ... })
		if err != nil {
			return err
		}
	*/

	// Phase 2: Merge
	// We will soon update merger.Merge to accept ctx and a progress callback
	/*
		err = merger.Merge(ctx, tempFiles, output, merger.Options[T]{ ... })
		if err != nil {
			return err
		}
	*/

	return nil // placeholder until we wire the internals
}
