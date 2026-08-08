package mergeheap_test

import (
	stdheap "container/heap"
	"testing"

	"github.com/stanimirivanov/etlstream/extsort/internal/mergeheap"
)

func TestRecordHeap(t *testing.T) {
	cmp := func(a, b int) int {
		return a - b
	}

	rh := &mergeheap.RecordHeap[int]{
		Items: []mergeheap.Item[int]{
			{Record: 10, ReaderIdx: 0},
			{Record: 3, ReaderIdx: 1},
			{Record: 7, ReaderIdx: 2},
		},
		Cmp: cmp,
	}

	stdheap.Init(rh)

	expected := []int{3, 7, 10}
	for _, exp := range expected {
		if rh.Len() == 0 {
			t.Fatalf("expected item %d, but heap is empty", exp)
		}
		item := stdheap.Pop(rh).(mergeheap.Item[int])
		if item.Record != exp {
			t.Errorf("got %d, want %d", item.Record, exp)
		}
	}
}
