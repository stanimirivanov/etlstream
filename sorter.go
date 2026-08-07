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
	Serializer  Serializer[T]
	Comparator  Comparator[T]
	MaxItems    int
	TempDir     string
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

	// Phase A: Split & Sort
	tempFiles, err := s.splitPhase(input)
	if err != nil {
		return fmt.Errorf("split phase failed: %w", err)
	}

	// Ensure temporary files are deleted after the merge is complete or if it fails
	defer func() {
		for _, file := range tempFiles {
			_ = os.Remove(file)
		}
	}()

	// Phase B: K-Way Merge
	return s.mergePhase(tempFiles, output)
}

// =====================================================================================
// PHASE A: SPLIT & SORT (Pipeline, Fan-Out, Fan-In)
// =====================================================================================

type fileResult struct {
	path string
	err  error
}

func (s *Sorter[T]) splitPhase(input io.Reader) ([]string, error) {
	reader, err := s.Serializer.CreateReader(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	maxItems, concurrency := s.getThresholds()
	chunksCh := make(chan []T, concurrency)
	fileResCh := make(chan fileResult, concurrency)
	readErrCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Stage 1: Producer
	go s.startProducer(reader, maxItems, chunksCh, readErrCh)

	// Stage 2: Fan-Out Worker Pool
	s.startWorkers(concurrency, chunksCh, fileResCh, &wg)

	// Stage 3: Fan-In Collector
	return s.collectResults(fileResCh, readErrCh, &wg)
}

// Stage 1: Reads chunks of records and pipes them to workers
func (s *Sorter[T]) startProducer(reader Reader[T], maxItems int, chunksCh chan<- []T, readErrCh chan<- error) {
	defer close(chunksCh)
	defer close(readErrCh)

	for {
		chunk, err := s.readChunk(reader, maxItems)
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

// Helper: extracts inner loop logic to lower cyclomatic complexity
func (s *Sorter[T]) readChunk(reader Reader[T], maxItems int) ([]T, error) {
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

// Stage 2: Spawns worker pool to sort and write concurrently
func (s *Sorter[T]) startWorkers(concurrency int, chunksCh <-chan []T, fileResCh chan<- fileResult, wg *sync.WaitGroup) {
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
}

// writeChunk sorts a single slice in memory and flushes it to disk
func (s *Sorter[T]) writeChunk(chunk []T) (string, error) {
	slices.SortFunc(chunk, s.Comparator)

	file, err := os.CreateTemp(s.TempDir, "bigsorter-chunk-*")
	if err != nil {
		return "", fmt.Errorf("create temp file failed: %w", err)
	}
	defer file.Close()

	writer, err := s.Serializer.CreateWriter(file)
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

// Stage 3: Multiplexes results from all workers safely
func (s *Sorter[T]) collectResults(fileResCh chan fileResult, readErrCh <-chan error, wg *sync.WaitGroup) ([]string, error) {
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

	// Clean up valid files if any part of the process failed
	if splitErr != nil {
		for _, f := range tempFiles {
			_ = os.Remove(f)
		}
		return nil, splitErr
	}

	return tempFiles, nil
}

// =====================================================================================
// PHASE B: K-WAY MERGE (Generators & Pre-fetching)
// =====================================================================================

type recordResult[T any] struct {
	record T
	err    error
}

func (s *Sorter[T]) mergePhase(tempFiles []string, output io.Writer) error {
	if len(tempFiles) == 0 {
		return nil
	}

	files, genChans, err := s.setupGenerators(tempFiles)
	if err != nil {
		return err
	}
	// Close files to abruptly terminate generator goroutines if we exit early
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	rh, err := s.primeHeap(genChans)
	if err != nil {
		return err
	}
	stdheap.Init(rh)

	writer, err := s.Serializer.CreateWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer writer.Close()

	return s.executeKWayMerge(rh, genChans, writer)
}

// Setup: Open all temp files and attach a background generator to each
func (s *Sorter[T]) setupGenerators(tempFiles []string) ([]*os.File, []<-chan recordResult[T], error) {
	files := make([]*os.File, 0, len(tempFiles))
	genChans := make([]<-chan recordResult[T], 0, len(tempFiles))

	for _, path := range tempFiles {
		f, err := os.Open(path)
		if err != nil {
			return files, nil, fmt.Errorf("failed to open temp file %s: %w", path, err)
		}
		files = append(files, f)

		r, err := s.Serializer.CreateReader(f)
		if err != nil {
			return files, nil, fmt.Errorf("failed to create reader for %s: %w", path, err)
		}
		genChans = append(genChans, s.startGenerator(r, 16))
	}
	return files, genChans, nil
}

// Generator: Pre-fetches records from disk asynchronously
func (s *Sorter[T]) startGenerator(r Reader[T], bufSize int) <-chan recordResult[T] {
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

// Bootstraps the Min-Heap with the first item from each generator
func (s *Sorter[T]) primeHeap(genChans []<-chan recordResult[T]) (*mergeheap.RecordHeap[T], error) {
	rh := &mergeheap.RecordHeap[T]{
		Items: make([]mergeheap.Item[T], 0, len(genChans)),
		Cmp:   s.Comparator,
	}

	for i, ch := range genChans {
		res := <-ch
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				continue
			}
			return nil, fmt.Errorf("failed to prime heap from temp file %d: %w", i, res.err)
		}
		rh.Items = append(rh.Items, mergeheap.Item[T]{Record: res.record, ReaderIdx: i})
	}
	return rh, nil
}

// Executes the hot-loop of the merge algorithm
func (s *Sorter[T]) executeKWayMerge(rh *mergeheap.RecordHeap[T], genChans []<-chan recordResult[T], writer Writer[T]) error {
	for rh.Len() > 0 {
		minItem := stdheap.Pop(rh).(mergeheap.Item[T])

		if err := writer.Write(minItem.Record); err != nil {
			return fmt.Errorf("failed to write record during merge: %w", err)
		}

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

// getThresholds normalizes default values if the user left them unset
func (s *Sorter[T]) getThresholds() (maxItems, concurrency int) {
	maxItems = s.MaxItems
	if maxItems <= 0 {
		maxItems = 100_000
	}
	concurrency = s.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	return maxItems, concurrency
}
