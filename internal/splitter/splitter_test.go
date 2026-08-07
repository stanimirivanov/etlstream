package splitter_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stanimirivanov/bigsorter/internal/splitter"
	"github.com/stanimirivanov/bigsorter/types"
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
		Concurrency: 2,
	}

	tempFiles, err := splitter.Split(bytes.NewBufferString(inputData), opts)
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

	// Verify each temp file contains sorted records
	for i, fpath := range tempFiles {
		content, err := os.ReadFile(fpath)
		if err != nil {
			t.Fatalf("failed to read temp file %s: %v", fpath, err)
		}
		lines := strings.Split(strings.TrimSpace(string(content)), "\n")

		if i == 0 { // chunk 1: delta, alpha -> alpha, delta
			if len(lines) != 2 || lines[0] != "alpha" || lines[1] != "delta" {
				t.Errorf("chunk 0 mismatch: %v", lines)
			}
		} else if i == 1 { // chunk 2: charlie, bravo -> bravo, charlie
			if len(lines) != 2 || lines[0] != "bravo" || lines[1] != "charlie" {
				t.Errorf("chunk 1 mismatch: %v", lines)
			}
		} else if i == 2 { // chunk 3: echo -> echo
			if len(lines) != 1 || lines[0] != "echo" {
				t.Errorf("chunk 2 mismatch: %v", lines)
			}
		}
	}
}

func TestSplitter_Validation(t *testing.T) {
	_, err := splitter.Split[string](bytes.NewBufferString("test"), splitter.Options[string]{})
	if err == nil {
		t.Error("expected error for missing serializer/comparator, got nil")
	}
}
