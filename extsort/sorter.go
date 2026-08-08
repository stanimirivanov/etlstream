package extsort

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/stanimirivanov/etlstream/extsort/internal/merger"
	"github.com/stanimirivanov/etlstream/extsort/internal/splitter"
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
}

// Sort reads the entire input, sorts it externally, and writes to output.
func (s *Sorter[T]) Sort(input io.Reader, output io.Writer) error {
	if s.Serializer == nil {
		return errors.New("serializer is required")
	}
	if s.Comparator == nil {
		return errors.New("comparator is required")
	}

	// Phase A: Split & Sort into temporary chunk files
	tempFiles, err := splitter.Split(input, splitter.Options[T]{
		Serializer:  s.Serializer,
		Comparator:  s.Comparator,
		MaxItems:    s.MaxItems,
		Concurrency: s.Concurrency,
		TempDir:     s.TempDir,
	})
	if err != nil {
		return fmt.Errorf("split phase failed: %w", err)
	}

	// Ensure temporary files are cleaned up upon completion or failure
	defer func() {
		for _, file := range tempFiles {
			_ = os.Remove(file)
		}
	}()

	// Phase B: K-Way Merge into final output
	if err := merger.Merge(tempFiles, output, merger.Options[T]{
		Serializer: s.Serializer,
		Comparator: s.Comparator,
	}); err != nil {
		return fmt.Errorf("merge phase failed: %w", err)
	}

	return nil
}
