package fixed_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/stanimirivanov/bigsorter/fixed"
)

func TestFixedSizeRecordSerializer(t *testing.T) {
	s := fixed.Serializer{Size: 3}
	input := []byte("ABCDEF123")

	r, err := s.CreateReader(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("create reader error: %v", err)
	}

	var records [][]byte
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read error: %v", err)
		}
		records = append(records, rec)
	}

	expected := [][]byte{
		[]byte("ABC"),
		[]byte("DEF"),
		[]byte("123"),
	}

	if !reflect.DeepEqual(records, expected) {
		t.Errorf("got %v, want %v", records, expected)
	}

	out := &bytes.Buffer{}
	w, err := s.CreateWriter(out)
	if err != nil {
		t.Fatalf("create writer error: %v", err)
	}

	for _, rec := range records {
		if err := w.Write(rec); err != nil {
			t.Fatalf("write error: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("got %s, want %s", out.Bytes(), input)
	}
}
