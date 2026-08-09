package extsort

import (
	"context"
	"io"

	"github.com/stanimirivanov/etlstream/extsort/internal/merger"
	"github.com/stanimirivanov/etlstream/extsort/internal/splitter"
	"github.com/stanimirivanov/etlstream/format/types"
)

// Sorter orchestrates the external sorting process.
type Sorter[T any] struct {
	// Serializer converts the generic type T to and from bytes.
	Serializer types.Serializer[T]

	// Comparator defines the sorting order for type T.
	Comparator types.Comparator[T]

	// MaxItems defines the maximum number of records kept in memory per worker.
	MaxItems int

	// Concurrency sets the number of concurrent goroutines for the split phase.
	Concurrency int

	// TempDir specifies where temporary chunk files will be written.
	TempDir string

	// Unique determines if consecutive duplicate records should be dropped.
	Unique bool

	// ProgressFunc is called periodically with metrics if provided.
	ProgressFunc ProgressFunc
}

// Sort reads records from the input, sorts them, and writes to the output.
// It uses context.Background() by default.
func (s *Sorter[T]) Sort(input io.Reader, output io.Writer) error {
	return s.SortContext(context.Background(), input, output)
}

// SortContext performs the external sort but listens for cancellation signals
// and reports progress back to the caller.
func (s *Sorter[T]) SortContext(ctx context.Context, input io.Reader, output io.Writer) error {
	// ---------------------------------------------------------
	// PHASE 1: SPLIT
	// ---------------------------------------------------------
	splitOpts := splitter.Options[T]{
		Serializer:  s.Serializer,
		Comparator:  s.Comparator,
		MaxItems:    s.MaxItems,
		Concurrency: s.Concurrency,
		TempDir:     s.TempDir,
	}

	// Map the internal lock-free callback to the public struct
	if s.ProgressFunc != nil {
		splitOpts.OnProgress = func(recordsRead int64, filesCount int) {
			s.ProgressFunc(Progress{
				Phase:          PhaseSplit,
				RecordsRead:    recordsRead,
				TempFilesCount: filesCount,
			})
		}
	}

	tempFiles, err := splitter.Split(ctx, input, splitOpts)
	if err != nil {
		return err
	}

	// ---------------------------------------------------------
	// PHASE 2: MERGE
	// ---------------------------------------------------------
	mergeOpts := merger.Options[T]{
		Serializer: s.Serializer,
		Comparator: s.Comparator,
		Unique:     s.Unique,
	}

	// Map the internal lock-free callback to the public struct
	if s.ProgressFunc != nil {
		mergeOpts.OnProgress = func(recordsProcessed int64) {
			s.ProgressFunc(Progress{
				Phase:          PhaseMerge,
				RecordsRead:    recordsProcessed,
				TempFilesCount: len(tempFiles),
			})
		}
	}

	return merger.Merge(ctx, tempFiles, output, mergeOpts)
}
