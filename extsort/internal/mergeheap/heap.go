package mergeheap

import (
	"container/heap"
	"fmt"
	"os"

	"github.com/stanimirivanov/etlstream/format/types"
)

// Item represents a single record and the stream it came from.
// We expose Record and Reader publicly so the Merger can access them directly.
type Item[T any] struct {
	Record T
	Reader types.Reader[T]
}

// internalHeap implements the container/heap.Interface using any.
// We keep this unexported so the rest of the application doesn't have to deal with type assertions.
type internalHeap[T any] struct {
	items      []*Item[T]
	comparator types.Comparator[T]
}

func (h *internalHeap[T]) Len() int { return len(h.items) }

func (h *internalHeap[T]) Less(i, j int) bool {
	return h.comparator(h.items[i].Record, h.items[j].Record) < 0
}

func (h *internalHeap[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *internalHeap[T]) Push(x any) {
	h.items = append(h.items, x.(*Item[T]))
}

func (h *internalHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// Heap is the public, type-safe wrapper used by the K-Way Merger.
type Heap[T any] struct {
	inner *internalHeap[T]
}

// New initializes the heap by opening readers for all temporary files
// and reading the very first record from each to seed the Min-Heap.
func New[T any](files []*os.File, serializer types.Serializer[T], comparator types.Comparator[T]) (*Heap[T], error) {
	inner := &internalHeap[T]{
		items:      make([]*Item[T], 0, len(files)),
		comparator: comparator,
	}

	for _, f := range files {
		reader, err := serializer.CreateReader(f)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader for heap: %w", err)
		}

		// Read the first record to seed the heap.
		// If we hit EOF immediately, we just don't add this file to the heap.
		record, err := reader.Read()
		if err == nil {
			inner.items = append(inner.items, &Item[T]{
				Record: record,
				Reader: reader,
			})
		}
	}

	heap.Init(inner)
	return &Heap[T]{inner: inner}, nil
}

// Len returns the current number of active streams in the heap.
func (h *Heap[T]) Len() int {
	return h.inner.Len()
}

// Push adds a new item to the heap in a type-safe manner.
func (h *Heap[T]) Push(item *Item[T]) {
	heap.Push(h.inner, item)
}

// Pop removes and returns the smallest item from the heap type-safely.
func (h *Heap[T]) Pop() *Item[T] {
	return heap.Pop(h.inner).(*Item[T])
}
