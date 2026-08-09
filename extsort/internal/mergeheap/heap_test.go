package mergeheap_test

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stanimirivanov/etlstream/extsort/internal/mergeheap"
	"github.com/stanimirivanov/etlstream/format/types"
)

// stringReader mocks a streaming file reader
type stringReader struct {
	items []string
	pos   int
}

func (r *stringReader) Read() (string, error) {
	if r.pos >= len(r.items) {
		return "", io.EOF
	}
	item := r.items[r.pos]
	r.pos++
	return item, nil
}

// mockSerializer creates readers seeded with specific test data
type mockSerializer struct {
	data [][]string
	idx  int
}

func (m *mockSerializer) CreateReader(r io.Reader) (types.Reader[string], error) {
	reader := &stringReader{items: m.data[m.idx]}
	m.idx++
	return reader, nil
}

func (m *mockSerializer) CreateWriter(w io.Writer) (types.Writer[string], error) {
	return nil, nil // Not needed for heap tests
}

func TestHeap_PopAndPush(t *testing.T) {
	// 1. Create dummy temporary files to satisfy the *os.File requirement
	f1, _ := os.CreateTemp("", "heap_test_*")
	f2, _ := os.CreateTemp("", "heap_test_*")
	defer os.Remove(f1.Name())
	defer os.Remove(f2.Name())
	defer f1.Close()
	defer f2.Close()

	// Stream 1 has "apple" and "cherry". Stream 2 has "banana" and "date".
	serializer := &mockSerializer{
		data: [][]string{
			{"apple", "cherry"},
			{"banana", "date"},
		},
	}

	// 2. Initialize the heap
	heap, err := mergeheap.New([]*os.File{f1, f2}, serializer, strings.Compare)
	if err != nil {
		t.Fatalf("failed to create heap: %v", err)
	}

	if heap.Len() != 2 {
		t.Fatalf("expected heap length 2, got %d", heap.Len())
	}

	// 3. Pop the smallest item (should be "apple" from Stream 1)
	item1 := heap.Pop()
	if item1.Record != "apple" {
		t.Errorf("expected 'apple', got '%s'", item1.Record)
	}

	// 4. Simulate the merger reading the next item from Stream 1 and pushing it back
	next1, _ := item1.Reader.Read()
	item1.Record = next1 // "cherry"
	heap.Push(item1)

	// 5. Pop the next smallest item (should be "banana" from Stream 2)
	item2 := heap.Pop()
	if item2.Record != "banana" {
		t.Errorf("expected 'banana', got '%s'", item2.Record)
	}
}
