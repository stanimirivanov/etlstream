package lines

import (
	"bufio"
	"io"

	"github.com/stanimirivanov/bigsorter"
)

// Serializer implements bigsorter.Serializer for plain text lines.
type Serializer struct{}

func (s Serializer) CreateReader(r io.Reader) (bigsorter.Reader[string], error) {
	return &reader{scanner: bufio.NewScanner(r)}, nil
}

func (s Serializer) CreateWriter(w io.Writer) (bigsorter.Writer[string], error) {
	return &writer{bw: bufio.NewWriter(w)}, nil
}

type reader struct {
	scanner *bufio.Scanner
}

func (r *reader) Read() (string, error) {
	if r.scanner.Scan() {
		return r.scanner.Text(), nil
	}
	if err := r.scanner.Err(); err != nil {
		return "", err
	}
	return "", io.EOF
}

type writer struct {
	bw *bufio.Writer
}

func (w *writer) Write(record string) error {
	if _, err := w.bw.WriteString(record); err != nil {
		return err
	}
	return w.bw.WriteByte('\n')
}

func (w *writer) Flush() error {
	return w.bw.Flush()
}

func (w *writer) Close() error {
	return w.Flush() // No footer needed for plain text
}
