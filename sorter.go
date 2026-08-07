package bigsorter

import (
	stdheap "container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

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
}

// Sort reads the entire input, sorts it externally, and writes to output.
func (s *Sorter[T]) Sort(input io.Reader, output io.Writer) error {
	if s.Serializer == nil {
		return errors.New("serializer is required")
	}
	if s.Comparator == nil {
		return errors.New("comparator is required")
	}

	// Split the large input into smaller, sorted temporary files
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

	// Merge the sorted chunks into the final output
	return s.merge(tempFiles, output)
}

// split streams the input, chunks it, sorts each chunk, and writes to disk.
func (s *Sorter[T]) split(input io.Reader) ([]string, error) {
	reader, err := s.Serializer.CreateReader(input)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	maxItems := s.MaxItems
	if maxItems <= 0 {
		maxItems = 100_000 // Default chunk size
	}

	var tempFiles []string
	chunk := make([]T, 0, maxItems)

	for {
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return tempFiles, fmt.Errorf("read error during split: %w", err)
		}

		chunk = append(chunk, record)

		// When the chunk reaches the memory limit, sort it and flush to disk
		if len(chunk) >= maxItems {
			filePath, err := s.writeChunk(chunk)
			if err != nil {
				return tempFiles, err
			}
			tempFiles = append(tempFiles, filePath)
			chunk = chunk[:0] // Reset length, reuse capacity
		}
	}

	// Process any remaining items in the last, partially-filled chunk
	if len(chunk) > 0 {
		filePath, err := s.writeChunk(chunk)
		if err != nil {
			return tempFiles, err
		}
		tempFiles = append(tempFiles, filePath)
	}

	return tempFiles, nil
}

// writeChunk sorts a single slice of records in memory and saves it to a temp file.
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

// --- Min-Heap Implementation for K-Way Merge ---

// heapItem pairs a record with the index of the reader it came from.
type heapItem[T any] struct {
	record    T
	readerIdx int
}

// recordHeap implements container/heap for heapItem[T].
type recordHeap[T any] struct {
	items []heapItem[T]
	cmp   Comparator[T]
}

func (h *recordHeap[T]) Len() int { return len(h.items) }
func (h *recordHeap[T]) Less(i, j int) bool {
	// Min-heap: returns true if items[i] < items[j]
	return h.cmp(h.items[i].record, h.items[j].record) < 0
}
func (h *recordHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *recordHeap[T]) Push(x any)    { h.items = append(h.items, x.(heapItem[T])) }
func (h *recordHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// --- K-Way Merge Logic ---

// merge takes the sorted temporary files and combines them into the final output.
func (s *Sorter[T]) merge(tempFiles []string, output io.Writer) error {
	if len(tempFiles) == 0 {
		return nil
	}

	files := make([]*os.File, 0, len(tempFiles))
	readers := make([]Reader[T], 0, len(tempFiles))

	// Ensure all temporary file handles are closed when merging finishes
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	// Open all temp files and initialize readers
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

	// Initialize the Priority Queue (Min-Heap)
	rh := &mergeheap.RecordHeap[T]{
		Items: make([]mergeheap.Item[T], 0, len(readers)),
		Cmp:   s.Comparator,
	}

	// Prime the heap with the first record from each temporary file
	for i, r := range readers {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return fmt.Errorf("failed to prime heap from temp file %d: %w", i, err)
		}
		rh.Items = append(rh.Items, mergeheap.Item[T]{Record: record, ReaderIdx: i})
	}
	stdheap.Init(rh)

	// Prepare the final output writer
	writer, err := s.Serializer.CreateWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	// Flush and close the final writer to ensure footers are written
	defer writer.Close()

	// Execute the K-Way Merge
	for rh.Len() > 0 {
		// Pop the smallest item globally
		minItem := stdheap.Pop(rh).(mergeheap.Item[T])

		// Write it to the output file
		if err := writer.Write(minItem.Record); err != nil {
			return fmt.Errorf("failed to write record during merge: %w", err)
		}

		// Read the next record from the specific file that the smallest item just came from
		nextRecord, err := readers[minItem.ReaderIdx].Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Exhausted this chunk, do not push back to heap
				continue
			}
			return fmt.Errorf("merge read error on temp file %d: %w", minItem.ReaderIdx, err)
		}

		// Push the newly read record into the heap to be sorted against the others
		stdheap.Push(rh, mergeheap.Item[T]{Record: nextRecord, ReaderIdx: minItem.ReaderIdx})
	}

	return nil
}
