package lines_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stanimirivanov/bigsorter/lines"
)

func TestLinesSerializer(t *testing.T) {
	s := lines.Serializer{}

	inputData := "banana\napple\ncherry\n"
	r, err := s.CreateReader(bytes.NewBufferString(inputData))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	var readLines []string
	for {
		line, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("unexpected read error: %v", err)
		}
		readLines = append(readLines, line)
	}

	if len(readLines) != 3 || readLines[0] != "banana" || readLines[1] != "apple" || readLines[2] != "cherry" {
		t.Errorf("read mismatched lines: %v", readLines)
	}

	out := &bytes.Buffer{}
	w, err := s.CreateWriter(out)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for _, l := range readLines {
		if err := w.Write(l); err != nil {
			t.Fatalf("failed to write line: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	if out.String() != inputData {
		t.Errorf("got output %q, want %q", out.String(), inputData)
	}
}
