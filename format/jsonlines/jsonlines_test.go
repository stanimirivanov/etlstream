package jsonlines_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/stanimirivanov/etlstream/format/jsonlines"
)

type LogEntry struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
}

func TestJsonLinesSerializer(t *testing.T) {
	s := jsonlines.Serializer[LogEntry]{}
	input := `{"level":"info","msg":"started"}
{"level":"error","msg":"failed"}
`

	r, err := s.CreateReader(bytes.NewBufferString(input))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	var entries []LogEntry
	for {
		entry, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read error: %v", err)
		}
		entries = append(entries, entry)
	}

	expected := []LogEntry{
		{Level: "info", Message: "started"},
		{Level: "error", Message: "failed"},
	}

	if !reflect.DeepEqual(entries, expected) {
		t.Errorf("got %v, want %v", entries, expected)
	}

	out := &bytes.Buffer{}
	w, err := s.CreateWriter(out)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for _, e := range entries {
		if err := w.Write(e); err != nil {
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
