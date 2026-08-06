package csv

import (
	stdcsv "encoding/csv"
	"io"

	"github.com/stanimirivanov/bigsorter"
)

// Serializer implements bigsorter.Serializer for CSV records (as string slices).
// Allows overriding the default ',' delimiter
type Serializer struct {
	Comma rune
}

func (s Serializer) CreateReader(r io.Reader) (bigsorter.Reader[[]string], error) {
	cr := stdcsv.NewReader(r)
	if s.Comma != 0 {
		cr.Comma = s.Comma
	}
	return &reader{cr: cr}, nil
}

func (s Serializer) CreateWriter(w io.Writer) (bigsorter.Writer[[]string], error) {
	cw := stdcsv.NewWriter(w)
	if s.Comma != 0 {
		cw.Comma = s.Comma
	}
	return &writer{cw: cw}, nil
}

type reader struct {
	cr *stdcsv.Reader
}

func (r *reader) Read() ([]string, error) {
	return r.cr.Read()
}

type writer struct {
	cw *stdcsv.Writer
}

func (w *writer) Write(record []string) error {
	return w.cw.Write(record)
}

func (w *writer) Flush() error {
	w.cw.Flush()
	return w.cw.Error()
}

func (w *writer) Close() error {
	return w.Flush()
}
