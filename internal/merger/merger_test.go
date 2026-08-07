package merger_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stanimirivanov/bigsorter/internal/merger"
	"github.com/stanimirivanov/bigsorter/types"
)

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

func createTempChunk(t *testing.T, lines []string) string {
	t.Helper()
	file, err := os.CreateTemp("", "merger-test-chunk-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer file.Close()

	for _, l := range lines {
		if _, err := file.WriteString(l + "\n"); err != nil {
			t.Fatalf("failed to write to temp file: %v", err)
		}
	}
	return file.Name()
}

func TestMerger_KWayMerge(t *testing.T) {
	// Pre-sorted chunk 1: alpha, delta
	f1 := createTempChunk(t, []string{"alpha", "delta"})
	// Pre-sorted chunk 2: bravo, charlie
	f2 := createTempChunk(t, []string{"bravo", "charlie"})
	// Pre-sorted chunk 3: echo, foxtrot
	f3 := createTempChunk(t, []string{"echo", "foxtrot"})

	tempFiles := []string{f1, f2, f3}
	defer func() {
		for _, f := range tempFiles {
			_ = os.Remove(f)
		}
	}()

	opts := merger.Options[string]{
		Serializer: StringSerializer{},
		Comparator: strings.Compare,
	}

	out := &bytes.Buffer{}
	err := merger.Merge(tempFiles, out, opts)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	expected := "alpha\nbravo\ncharlie\ndelta\necho\nfoxtrot\n"
	if out.String() != expected {
		t.Errorf("got:\n%q\nwant:\n%q", out.String(), expected)
	}
}

func TestMerger_Validation(t *testing.T) {
	out := &bytes.Buffer{}
	err := merger.Merge[string]([]string{"dummy"}, out, merger.Options[string]{})
	if err == nil {
		t.Error("expected error for missing options, got nil")
	}
}
