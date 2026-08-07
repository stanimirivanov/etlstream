package splitter

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sync"
)

// Reader abstracts record streaming for the splitter.
type Reader[T any] interface {
	Read() (T, error)
}

// Writer abstracts writing records to disk for the splitter.
type Writer[T any] interface {
	Write(record T) error
	Close() error
}

// Serializer creates readers and writers for type T.
type Serializer[T any] interface {
	CreateReader(r io.Reader) (Reader[T], error)
	CreateWriter(w io.Writer) (Writer[T], error)
}

// Comparator compares two records,
// returning <0 if a < b, 0 if equal, >0 if a > b.
type Comparator[T any] func(a, b T) int

// Options configures the split phase execution.
type Options[T any] struct {
	Serializer  Serializer[T]
	Comparator  Comparator[T]
	MaxItems    int
	Concurrency int
	TempDir     string
}

type fileResult struct {
	path string
	err  error
}

// Split reads input records, breaks them into sorted chunks,
// and flushes them to temp files.
func Split[T any](input io.Reader, opts Options[T]) ([]string, error) {
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

	// Stage 1: Producer
	go startProducer(reader, maxItems, chunksCh, readErrCh)

	// Stage 2: Fan-Out Worker Pool
	startWorkers(concurrency, chunksCh, fileResCh, &wg, opts)

	// Stage 3: Fan-In Collector
	return collectResults(fileResCh, readErrCh, &wg)
}

func startProducer[T any](reader Reader[T], maxItems int, chunksCh chan<- []T, readErrCh chan<- error) {
	defer close(chunksCh)
	defer close(readErrCh)

	for {
		chunk, err := readChunk(reader, maxItems)
		if len(chunk) > 0 {
			chunksCh <- chunk
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErrCh <- fmt.Errorf("read error: %w", err)
			}
			return
		}
	}
}

func readChunk[T any](reader Reader[T], maxItems int) ([]T, error) {
	chunk := make([]T, 0, maxItems)
	for len(chunk) < maxItems {
		record, err := reader.Read()
		if err != nil {
			return chunk, err
		}
		chunk = append(chunk, record)
	}
	return chunk, nil
}

func startWorkers[T any](concurrency int, chunksCh <-chan []T, fileResCh chan<- fileResult, wg *sync.WaitGroup, opts Options[T]) {
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunksCh {
				filePath, err := writeChunk(chunk, opts)
				fileResCh <- fileResult{path: filePath, err: err}
			}
		}()
	}
}

func writeChunk[T any](chunk []T, opts Options[T]) (string, error) {
	slices.SortFunc(chunk, opts.Comparator)

	file, err := os.CreateTemp(opts.TempDir, "bigsorter-chunk-*")
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
