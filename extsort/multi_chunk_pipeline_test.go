package extsort_test

import (
	"bytes"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stanimirivanov/etlstream/extsort"
	"github.com/stanimirivanov/etlstream/format/lines"
)

// randomWords returns n deterministic random lowercase words.
func randomWords(n int) []string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	r := rand.New(rand.NewSource(1))
	words := make([]string, n)
	for i := range words {
		words[i] = string([]byte{byte('a' + r.Intn(len(letters)))})
	}
	return words
}

// TestSorter_MultiChunkFanOut forces several chunks (MaxItems small vs. a
// large corpus) at Concurrency >= 2, then asserts the full 3-stage split
// pipeline (producer -> fan-out worker pool -> fan-in collector -> merge)
// emits the complete sorted stream.
func TestSorter_MultiChunkFanOut(t *testing.T) {
	const n = 4000
	const maxItems = 256 // => ~16 chunks, fan-out exercised
	const concurrency = 2

	words := randomWords(n)
	want := append([]string(nil), words...)
	sort.Strings(want)

	s := extsort.Sorter[string]{
		Serializer:  lines.Serializer{},
		Comparator:  strings.Compare,
		MaxItems:    maxItems,
		Concurrency: concurrency,
	}

	out := &bytes.Buffer{}
	if err := s.Sort(strings.NewReader(joinLines(words)), out); err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if got := out.String(); got != joinLines(want) {
		t.Errorf("sort order mismatch\n got=%q\nwant=%q", got, joinLines(want))
	}
}

// joinLines concatenates words with a newline between them, mirroring the
// plain-text line format the lines.Serializer reads/writes.
func joinLines(words []string) string {
	return strings.Join(words, "\n") + "\n"
}

// BenchmarkSorter_MultiChunkFanOut benchmarks the full 3-stage pipeline
// (split fan-out + fan-in collector + K-way merge) for a fixed corpus. It
// records throughput for the fan-out + fan-in path.
func BenchmarkSorter_MultiChunkFanOut(b *testing.B) {
	words := randomWords(4000)
	input := joinLines(words)
	s := extsort.Sorter[string]{
		Serializer:  lines.Serializer{},
		Comparator:  strings.Compare,
		MaxItems:    256,
		Concurrency: 2,
	}
	var out bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out.Reset()
		if err := s.Sort(strings.NewReader(input), &out); err != nil {
			b.Fatalf("Sort: %v", err)
		}
	}
}
