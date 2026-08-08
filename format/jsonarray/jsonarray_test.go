package jsonarray_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/stanimirivanov/etlstream/format/jsonarray"
)

type Item struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestJsonArraySerializer(t *testing.T) {
	s := jsonarray.Serializer[Item]{}
	input := `[{"id":2,"name":"Bob"},{"id":1,"name":"Alice"}]`

	r, err := s.CreateReader(bytes.NewBufferString(input))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	var items []Item
	for {
		item, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read error: %v", err)
		}
		items = append(items, item)
	}

	expected := []Item{
		{ID: 2, Name: "Bob"},
		{ID: 1, Name: "Alice"},
	}

	if !reflect.DeepEqual(items, expected) {
		t.Errorf("read items = %v, want %v", items, expected)
	}

	out := &bytes.Buffer{}
	w, err := s.CreateWriter(out)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for _, it := range items {
		if err := w.Write(it); err != nil {
			t.Fatalf("write error: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	r2, err := s.CreateReader(out)
	if err != nil {
		t.Fatalf("failed to create reader for output: %v", err)
	}
	var items2 []Item
	for {
		it, err := r2.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read error: %v", err)
		}
		items2 = append(items2, it)
	}

	if !reflect.DeepEqual(items2, expected) {
		t.Errorf("roundtrip items = %v, want %v", items2, expected)
	}
}
