package merger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/stanimirivanov/etlstream/extsort/internal/mergeheap"
	"github.com/stanimirivanov/etlstream/format/types"
)

// Options configures the merge phase execution.
type Options[T any] struct {
	Serializer types.Serializer[T]
	Comparator types.Comparator[T]
	Unique     bool // Drops consecutive duplicate records
	// OnProgress is a lock-free callback primitive to report metrics upstream
	OnProgress func(recordsProcessed int64)
}

// Merge reads from multiple sorted temporary files and multiplexes them
// into a single globally sorted output stream using a Min-Heap.
func Merge[T any](ctx context.Context, tempFiles []string, output io.Writer, opts Options[T]) error {
	if opts.Serializer == nil {
		return errors.New("serializer is required")
	}
	if opts.Comparator == nil {
		return errors.New("comparator is required")
	}
	if len(tempFiles) == 0 {
		return nil // Nothing to merge
	}

	// 1. Open all temp files and create readers
	files := make([]*os.File, 0, len(tempFiles))

	// Defer cleanup: close and remove all temporary files
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
		for _, path := range tempFiles {
			_ = os.Remove(path)
		}
	}()

	for _, path := range tempFiles {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open temp file %s: %w", path, err)
		}
		files = append(files, f)
	}

	// 2. Initialize the Min-Heap
	heap, err := mergeheap.New(files, opts.Serializer, opts.Comparator)
	if err != nil {
		return fmt.Errorf("failed to initialize merge heap: %w", err)
	}

	writer, err := opts.Serializer.CreateWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer writer.Close()

	// 3. Setup Progress Reporting
	var recordsProcessed int64
	var progressDone chan struct{}

	if opts.OnProgress != nil {
		progressDone = make(chan struct{})
		go startProgressReporter(progressDone, &recordsProcessed, opts.OnProgress)
	}

	defer func() {
		if progressDone != nil {
			close(progressDone)
			// Trigger one final update on exit
			opts.OnProgress(atomic.LoadInt64(&recordsProcessed))
		}
	}()

	// 4. The Merge Loop
	var hasLast bool
	var lastRecord T

	for heap.Len() > 0 {
		// Batch polling: Check context every 10,000 records to save CPU cycles
		if recordsProcessed%10000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		item := heap.Pop()

		// Deduplication Logic (Phase 1)
		if opts.Unique && hasLast {
			if opts.Comparator(lastRecord, item.Record) == 0 {
				// It's a duplicate. Advance the reader but skip writing.
				if err := advanceStream(heap, item); err != nil {
					return err
				}
				continue
			}
		}

		// Write to the final output
		if err := writer.Write(item.Record); err != nil {
			return fmt.Errorf("write error during merge: %w", err)
		}

		lastRecord = item.Record
		hasLast = true
		atomic.AddInt64(&recordsProcessed, 1)

		// Advance the stream that we just popped from
		if err := advanceStream(heap, item); err != nil {
			return err
		}
	}

	return nil
}

// advanceStream reads the next record from the popped item's stream
// and pushes it back into the heap if not EOF.
func advanceStream[T any](heap *mergeheap.Heap[T], item *mergeheap.Item[T]) error {
	nextRecord, err := item.Reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil // Stream exhausted, do not push back
		}
		return fmt.Errorf("read error during merge: %w", err)
	}

	item.Record = nextRecord
	heap.Push(item)
	return nil
}

// startProgressReporter triggers the callback periodically without blocking the loop.
func startProgressReporter(done <-chan struct{}, recordsProcessed *int64, onProgress func(int64)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			onProgress(atomic.LoadInt64(recordsProcessed))
		case <-done:
			return
		}
	}
}
