package splitter_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stanimirivanov/etlstream/extsort/internal/splitter"
	"github.com/stanimirivanov/etlstream/format/types"
)

// StringSerializer implements splitter.Serializer for test strings
type StringSerializer struct{}

type stringReader struct {
	scanner *bufio.Scanner
}

func (r *stringReader) Read() (string, error) {
	if r.scanner.Scan() {
		return r.scanner.Text(), nil
	}
	if err := r.scanner.Err(); err != nil {
		return "", err
	}
	return "", io.EOF
}

type stringWriter struct {
	writer *bufio.Writer
	file   io.Closer
}

func (w *stringWriter) Write(s string) error {
	_, err := w.writer.WriteString(s + "\n")
	return err
}

func (w *stringWriter) Flush() error {
	return w.writer.Flush()
}

func (w *stringWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func (s StringSerializer) CreateReader(r io.Reader) (types.Reader[string], error) {
	return &stringReader{scanner: bufio.NewScanner(r)}, nil
}

func (s StringSerializer) CreateWriter(w io.Writer) (types.Writer[string], error) {
	closer, _ := w.(io.Closer)
	return &stringWriter{writer: bufio.NewWriter(w), file: closer}, nil
}

func TestSplitter_ChunksAndSorts(t *testing.T) {
	inputData := "delta\nalpha\ncharlie\nbravo\necho\n"

	opts := splitter.Options[string]{
		Serializer:  StringSerializer{},
		Comparator:  strings.Compare,
		MaxItems:    2, // 5 records with MaxItems=2 should create 3 temp files
		Concurrency: 2, // Tests concurrent workers
	}

	// In TestSplitter_ChunksAndSorts
	tempFiles, err := splitter.Split(context.Background(), bytes.NewBufferString(inputData), opts)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	defer func() {
		for _, f := range tempFiles {
			_ = os.Remove(f)
		}
	}()

	if len(tempFiles) != 3 {
		t.Fatalf("expected 3 temp files, got %d", len(tempFiles))
	}

	// Define the exact sorted chunks we expect to see, regardless of file order
	expectedChunks := map[string]bool{
		"alpha,delta":   false, // represents Chunk 1
		"bravo,charlie": false, // represents Chunk 2
		"echo":          false, // represents Chunk 3
	}

	// Verify each temp file contains one of the expected sorted records
	for _, fpath := range tempFiles {
		content, err := os.ReadFile(fpath)
		if err != nil {
			t.Fatalf("failed to read temp file %s: %v", fpath, err)
		}

		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		chunkKey := strings.Join(lines, ",")

		if _, exists := expectedChunks[chunkKey]; !exists {
			t.Errorf("found unexpected chunk content: %v", lines)
		} else {
			expectedChunks[chunkKey] = true // Mark as found
		}
	}

	// Ensure all expected chunks were found
	for chunkKey, found := range expectedChunks {
		if !found {
			t.Errorf("expected chunk [%s] was not found in any temp file", chunkKey)
		}
	}
}

func TestSplitter_Validation(t *testing.T) {
	_, err := splitter.Split[string](context.Background(), bytes.NewBufferString("test"), splitter.Options[string]{})
	if err == nil {
		t.Error("expected error for missing serializer/comparator, got nil")
	}
}
