package bigsorter

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
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

	// 1. Split the massive input into smaller, sorted temporary files
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

	// 2. Merge the sorted chunks into the final output
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
	// 1. Sort the chunk in memory using the user-provided Comparator
	slices.SortFunc(chunk, s.Comparator)

	// 2. Create a unique temporary file
	file, err := os.CreateTemp(s.TempDir, "bigsorter-chunk-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	// 3. Create a writer using the format-specific Serializer
	writer, err := s.Serializer.CreateWriter(file)
	if err != nil {
		return "", fmt.Errorf("failed to create chunk writer: %w", err)
	}

	// Ensure Close() is called to flush data and write footers (like JSON ']')
	defer writer.Close()

	// 4. Stream the sorted records to the temporary file
	for _, record := range chunk {
		if err := writer.Write(record); err != nil {
			return "", fmt.Errorf("failed to write record to chunk: %w", err)
		}
	}

	return file.Name(), nil
}

// merge takes the sorted temporary files and combines them into the final output.
func (s *Sorter[T]) merge(tempFiles []string, output io.Writer) error {
	// TODO: Implement K-Way Merge using a Priority Queue / Min-Heap.
	return errors.New("merge phase not yet implemented")
}
