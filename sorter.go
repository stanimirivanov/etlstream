package bigsorter

import (
	stdheap "container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sync"

	"github.com/stanimirivanov/bigsorter/internal/mergeheap"
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

	// 1. Split the massive input into smaller, sorted temporary files concurrently
	tempFiles, err := s.split(input)
	if err != nil {
		return fmt.Errorf("split phase failed: %w", err)
	}

	// Ensure temporary files are deleted after the merge is complete or if it fails
	defer func() {
		for _, file := range tempFiles {
			_ = os.Remove(file)
		}
	}()

	// 2. Merge the sorted chunks into the final output using pre-fetching generators
	return s.merge(tempFiles, output)
}

// Helper struct to pass worker results back to the collector
type fileResult struct {
	path string
	err  error
}

// split implements a 3-stage pipeline (Producer -> Fan-Out Workers -> Collector)
func (s *Sorter[T]) split(input io.Reader) ([]string, error) {
	reader, err := s.Serializer.CreateReader(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	maxItems := s.MaxItems
	if maxItems <= 0 {
		maxItems = 100_000
	}

	concurrency := s.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	chunksCh := make(chan []T, concurrency)
	fileResCh := make(chan fileResult, concurrency)

	// Stage 1: Producer Goroutine (Streams records into chunk slices)
	var readErr error
	go func() {
		defer close(chunksCh)
		for {
			chunk := make([]T, 0, maxItems)
			for len(chunk) < maxItems {
				record, err := reader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						if len(chunk) > 0 {
							chunksCh <- chunk
						}
						return
					}
					readErr = fmt.Errorf("read error during split: %w", err)
					return
				}
				chunk = append(chunk, record)
			}
			chunksCh <- chunk
		}
	}()

	// Stage 2: Fan-Out Worker Pool (Sorts & Writes chunks concurrently)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunksCh {
				filePath, err := s.writeChunk(chunk)
				fileResCh <- fileResult{path: filePath, err: err}
			}
		}()
	}

	// Wait for workers and close collector channel
	go func() {
		wg.Wait()
		close(fileResCh)
	}()

	// Stage 3: Fan-In Collector
	var tempFiles []string
	var splitErr error

	for res := range fileResCh {
		if res.err != nil {
			if splitErr == nil {
				splitErr = res.err
			}
		} else if res.path != "" {
			tempFiles = append(tempFiles, res.path)
		}
	}

	if readErr != nil && splitErr == nil {
		splitErr = readErr
	}

	// Clean up if an error occurred during split
	if splitErr != nil {
		for _, f := range tempFiles {
			_ = os.Remove(f)
		}
		return nil, splitErr
	}

	return tempFiles, nil
}

func (s *Sorter[T]) writeChunk(chunk []T) (string, error) {
	slices.SortFunc(chunk, s.Comparator)

	file, err := os.CreateTemp(s.TempDir, "bigsorter-chunk-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	writer, err := s.Serializer.CreateWriter(file)
	if err != nil {
		return "", fmt.Errorf("failed to create chunk writer: %w", err)
	}
	defer writer.Close()

	for _, record := range chunk {
		if err := writer.Write(record); err != nil {
			return "", fmt.Errorf("failed to write record to chunk: %w", err)
		}
	}

	return file.Name(), nil
}

// Helper struct for generator channel outputs
type recordResult[T any] struct {
	record T
	err    error
}

// Generator pattern: Continuously pre-fetches records from disk in a background goroutine
func startGenerator[T any](r Reader[T], bufSize int) <-chan recordResult[T] {
	ch := make(chan recordResult[T], bufSize)
	go func() {
		defer close(ch)
		for {
			rec, err := r.Read()
			if err != nil {
				ch <- recordResult[T]{err: err}
				return
			}
			ch <- recordResult[T]{record: rec}
		}
	}()
	return ch
}

// merge combines sorted temporary files using pre-fetching generator channels and a Min-Heap
func (s *Sorter[T]) merge(tempFiles []string, output io.Writer) error {
	if len(tempFiles) == 0 {
		return nil
	}

	files := make([]*os.File, 0, len(tempFiles))
	readers := make([]Reader[T], 0, len(tempFiles))

	// Closing underlying file handles stops generator goroutines cleanly if merge exits early
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	for _, path := range tempFiles {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open temp file %s: %w", path, err)
		}
		files = append(files, f)

		r, err := s.Serializer.CreateReader(f)
		if err != nil {
			return fmt.Errorf("failed to create reader for %s: %w", path, err)
		}
		readers = append(readers, r)
	}

	// Start pre-fetching generator goroutines for each temp file
	genChans := make([]<-chan recordResult[T], len(readers))
	for i, r := range readers {
		genChans[i] = startGenerator(r, 16) // Buffer of 16 items pre-fetched per file
	}

	rh := &mergeheap.RecordHeap[T]{
		Items: make([]mergeheap.Item[T], 0, len(readers)),
		Cmp:   s.Comparator,
	}

	// Prime heap with the first pre-fetched record from each generator
	for i, ch := range genChans {
		res := <-ch
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				continue
			}
			return fmt.Errorf("failed to prime heap from temp file %d: %w", i, res.err)
		}
		rh.Items = append(rh.Items, mergeheap.Item[T]{Record: res.record, ReaderIdx: i})
	}
	stdheap.Init(rh)

	writer, err := s.Serializer.CreateWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer writer.Close()

	// K-Way Merge loop
	for rh.Len() > 0 {
		minItem := stdheap.Pop(rh).(mergeheap.Item[T])

		if err := writer.Write(minItem.Record); err != nil {
			return fmt.Errorf("failed to write record during merge: %w", err)
		}

		// Pull next pre-fetched record from the generator channel
		res := <-genChans[minItem.ReaderIdx]
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				continue
			}
			return fmt.Errorf("merge read error on temp file %d: %w", minItem.ReaderIdx, res.err)
		}

		stdheap.Push(rh, mergeheap.Item[T]{Record: res.record, ReaderIdx: minItem.ReaderIdx})
	}

	return nil
}
