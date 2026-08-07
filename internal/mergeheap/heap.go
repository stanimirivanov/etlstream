package mergeheap

// Item pairs a record with the index of the reader it came from.
type Item[T any] struct {
	Record    T
	ReaderIdx int
}

// RecordHeap implements standard library container/heap for Item[T].
// We use a raw func signature to prevent importing the parent bigsorter package
type RecordHeap[T any] struct {
	Items []Item[T]
	Cmp   func(a, b T) int
}

func (h *RecordHeap[T]) Len() int { return len(h.Items) }

// Min-heap: returns true if Items[i] < Items[j]
func (h *RecordHeap[T]) Less(i, j int) bool {
	return h.Cmp(h.Items[i].Record, h.Items[j].Record) < 0
}

func (h *RecordHeap[T]) Swap(i, j int) {
	h.Items[i], h.Items[j] = h.Items[j], h.Items[i]
}

func (h *RecordHeap[T]) Push(x any) {
	h.Items = append(h.Items, x.(Item[T]))
}

func (h *RecordHeap[T]) Pop() any {
	old := h.Items
	n := len(old)
	item := old[n-1]
	h.Items = old[0 : n-1]
	return item
}
