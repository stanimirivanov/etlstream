package fixed

import (
	"errors"
	"io"

	"github.com/stanimirivanov/etlstream/extsort"
)

// Serializer implements etlstream.Serializer for fixed-size byte chunks.
type Serializer struct {
	Size int // The strict byte size of each record
}

func (s Serializer) CreateReader(r io.Reader) (extsort.Reader[[]byte], error) {
	if s.Size <= 0 {
		return nil, errors.New("fixed record size must be greater than zero")
	}
	return &reader{r: r, size: s.Size}, nil
}

func (s Serializer) CreateWriter(w io.Writer) (extsort.Writer[[]byte], error) {
	if s.Size <= 0 {
		return nil, errors.New("fixed record size must be greater than zero")
	}
	return &writer{w: w, size: s.Size}, nil
}

type reader struct {
	r    io.Reader
	size int
}

func (r *reader) Read() ([]byte, error) {
	buf := make([]byte, r.size)
	_, err := io.ReadFull(r.r, buf)
	if err != nil {
		// io.ErrUnexpectedEOF means the file wasn't cleanly divisible by the fixed size.
		return nil, err
	}
	return buf, nil
}

type writer struct {
	w    io.Writer
	size int
}

func (w *writer) Write(record []byte) error {
	if len(record) != w.size {
		return errors.New("record length does not match fixed size")
	}
	_, err := w.w.Write(record)
	return err
}

func (w *writer) Flush() error { return nil }

func (w *writer) Close() error { return nil }
