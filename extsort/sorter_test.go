package extsort_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stanimirivanov/etlstream/extsort"
	"github.com/stanimirivanov/etlstream/format/jsonarray"
	"github.com/stanimirivanov/etlstream/format/lines"
)

func TestSorter_TextLines(t *testing.T) {
	inputData := "banana\napple\ndragonfruit\ncherry\nelderberry\n"
	expectedOutput := "apple\nbanana\ncherry\ndragonfruit\nelderberry\n"

	sorter := extsort.Sorter[string]{
		Serializer:  lines.Serializer{},
		Comparator:  strings.Compare,
		MaxItems:    2, // Force creation of 3 temporary chunk files to test K-Way merge
		Concurrency: 2,
	}

	out := &bytes.Buffer{}
	err := sorter.Sort(bytes.NewBufferString(inputData), out)
	if err != nil {
		t.Fatalf("sorting failed: %v", err)
	}

	if out.String() != expectedOutput {
		t.Errorf("got:\n%q\nwant:\n%q", out.String(), expectedOutput)
	}
}

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestSorter_JSONArrayStructs(t *testing.T) {
	inputJSON := `[{"name":"Charlie","age":35},{"name":"Alice","age":25},{"name":"Bob","age":30}]`

	sorter := extsort.Sorter[User]{
		Serializer: jsonarray.Serializer[User]{},
		Comparator: func(a, b User) int {
			return a.Age - b.Age // Sort by Age ascending
		},
		MaxItems:    1, // Force each record into its own temp file
		Concurrency: 2,
	}

	out := &bytes.Buffer{}
	err := sorter.Sort(bytes.NewBufferString(inputJSON), out)
	if err != nil {
		t.Fatalf("sorting JSON failed: %v", err)
	}

	// Verify order via JSON array reader
	r, err := jsonarray.Serializer[User]{}.CreateReader(out)
	if err != nil {
		t.Fatalf("failed to read output JSON: %v", err)
	}

	var users []User
	for {
		u, err := r.Read()
		if err != nil {
			break
		}
		users = append(users, u)
	}

	if len(users) != 3 {
		t.Fatalf("expected 3 users, got %d", len(users))
	}
	if users[0].Name != "Alice" || users[1].Name != "Bob" || users[2].Name != "Charlie" {
		t.Errorf("incorrect sort order: %+v", users)
	}
}

func TestSorter_EmptyInput(t *testing.T) {
	sorter := extsort.Sorter[string]{
		Serializer: lines.Serializer{},
		Comparator: strings.Compare,
		MaxItems:   10,
	}

	out := &bytes.Buffer{}
	err := sorter.Sort(bytes.NewBufferString(""), out)
	if err != nil {
		t.Fatalf("empty input sort failed: %v", err)
	}

	if out.Len() != 0 {
		t.Errorf("expected empty output, got %q", out.String())
	}
}
