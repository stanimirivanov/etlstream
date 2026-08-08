package splitter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stanimirivanov/etlstream/format/types"
)

// Options configures the split phase execution.
// OnProgress is a lock-free callback primitive to report metrics upstream
type Options[T any] struct {
	Serializer  types.Serializer[T]
	Comparator  types.Comparator[T]
	MaxItems    int
	Concurrency int
	TempDir     string
	OnProgress  func(recordsRead int64, filesCount int)
}

type fileResult struct {
	path string
	err  error
}

// Split reads input records, breaks them into sorted chunks,
// and flushes them to temp files while respecting context cancellation.
func Split[T any](ctx context.Context, input io.Reader, opts Options[T]) ([]string, error) {
	if opts.Serializer == nil {
		return nil, errors.New("serializer is required")
	}
	if opts.Comparator == nil {
		return nil, errors.New("comparator is required")
	}

	reader, err := opts.Serializer.CreateReader(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	maxItems, concurrency := normalizeThresholds(opts.MaxItems, opts.Concurrency)

	chunksCh := make(chan []T, concurrency)
	fileResCh := make(chan fileResult, concurrency)
	readErrCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Atomic counters for progress tracking
	var recordsRead int64
	var filesCount int32
	var progressDone chan struct{}

	if opts.OnProgress != nil {
		progressDone = make(chan struct{})
		go startProgressReporter(progressDone, &recordsRead, &filesCount, opts.OnProgress)
	}

	// Stage 1: Producer
	go startProducer(ctx, reader, maxItems, chunksCh, readErrCh, &recordsRead)

	// Stage 2: Fan-Out Worker Pool
	startWorkers(ctx, concurrency, chunksCh, fileResCh, &wg, opts, &filesCount)

	// Stage 3: Fan-In Collector
	tempFiles, splitErr := collectResults(fileResCh, readErrCh, &wg)

	// Teardown progress reporting
	if progressDone != nil {
		close(progressDone)
		if splitErr == nil {
			opts.OnProgress(atomic.LoadInt64(&recordsRead), int(atomic.LoadInt32(&filesCount)))
		}
	}

	return tempFiles, splitErr
}

// startProgressReporter triggers the callback periodically without blocking the I/O pipeline.
func startProgressReporter(done <-chan struct{}, recordsRead *int64, filesCount *int32, onProgress func(int64, int)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			onProgress(atomic.LoadInt64(recordsRead), int(atomic.LoadInt32(filesCount)))
		case <-done:
			return
		}
	}
}

func startProducer[T any](ctx context.Context, reader types.Reader[T], maxItems int, chunksCh chan<- []T, readErrCh chan<- error, recordsRead *int64) {
	defer close(chunksCh)
	defer close(readErrCh)

	for {
		chunk, err := readChunk(ctx, reader, maxItems)

		if len(chunk) > 0 {
			atomic.AddInt64(recordsRead, int64(len(chunk)))
			// Safely push to channel or cancel
			select {
			case <-ctx.Done():
				readErrCh <- ctx.Err()
				return
			case chunksCh <- chunk:
			}
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErrCh <- fmt.Errorf("read error: %w", err)
			}
			return
		}
	}
}

func readChunk[T any](ctx context.Context, reader types.Reader[T], maxItems int) ([]T, error) {
	chunk := make([]T, 0, maxItems)
	for len(chunk) < maxItems {
		// Batch polling: Check context every 10,000 iterations to save CPU cycles
		if len(chunk)%10000 == 0 {
			select {
			case <-ctx.Done():
				return chunk, ctx.Err()
			default:
			}
		}

		record, err := reader.Read()
		if err != nil {
			return chunk, err
		}
		chunk = append(chunk, record)
	}
	return chunk, nil
}

func startWorkers[T any](ctx context.Context, concurrency int, chunksCh <-chan []T, fileResCh chan<- fileResult, wg *sync.WaitGroup, opts Options[T], filesCount *int32) {
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunksCh {
				// Abort early if context is cancelled before writing
				select {
				case <-ctx.Done():
					fileResCh <- fileResult{err: ctx.Err()}
					return
				default:
				}

				filePath, err := writeChunk(chunk, opts)
				if err == nil {
					atomic.AddInt32(filesCount, 1)
				}
				fileResCh <- fileResult{path: filePath, err: err}
			}
		}()
	}
}

func writeChunk[T any](chunk []T, opts Options[T]) (string, error) {
	slices.SortFunc(chunk, opts.Comparator)

	file, err := os.CreateTemp(opts.TempDir, "etlstream-chunk-*")
	if err != nil {
		return "", fmt.Errorf("create temp file failed: %w", err)
	}
	defer file.Close()

	writer, err := opts.Serializer.CreateWriter(file)
	if err != nil {
		return "", fmt.Errorf("create chunk writer failed: %w", err)
	}
	defer writer.Close()

	for _, record := range chunk {
		if err := writer.Write(record); err != nil {
			return "", fmt.Errorf("write record failed: %w", err)
		}
	}

	return file.Name(), nil
}

func collectResults(fileResCh chan fileResult, readErrCh <-chan error, wg *sync.WaitGroup) ([]string, error) {
	go func() {
		wg.Wait()
		close(fileResCh)
	}()

	var tempFiles []string
	var splitErr error

	for res := range fileResCh {
		if res.err != nil && splitErr == nil {
			splitErr = res.err
		} else if res.path != "" {
			tempFiles = append(tempFiles, res.path)
		}
	}

	if err := <-readErrCh; err != nil && splitErr == nil {
		splitErr = err
	}

	// Graceful cleanup of orphaned files on failure or cancellation
	if splitErr != nil {
		for _, f := range tempFiles {
			_ = os.Remove(f)
		}
		return nil, splitErr
	}

	return tempFiles, nil
}

func normalizeThresholds(maxItems, concurrency int) (int, int) {
	if maxItems <= 0 {
		maxItems = 100_000
	}
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	return maxItems, concurrency
}
