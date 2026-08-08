package csv_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/stanimirivanov/etlstream/format/csv"
)

func TestCsvSerializer(t *testing.T) {
	s := csv.Serializer{}
	input := "name,age\nAlice,30\nBob,25\n"

	r, err := s.CreateReader(bytes.NewBufferString(input))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	var rows [][]string
	for {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("unexpected read error: %v", err)
		}
		rows = append(rows, row)
	}

	expected := [][]string{
		{"name", "age"},
		{"Alice", "30"},
		{"Bob", "25"},
	}

	if !reflect.DeepEqual(rows, expected) {
		t.Errorf("read rows = %v, want %v", rows, expected)
	}

	out := &bytes.Buffer{}
	w, err := s.CreateWriter(out)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for _, row := range rows {
		if err := w.Write(row); err != nil {
			t.Fatalf("write error: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	if out.String() != input {
		t.Errorf("got %q, want %q", out.String(), input)
	}
}
