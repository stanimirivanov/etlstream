package bigsorter_test

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/stanimirivanov/bigsorter"
	"github.com/stanimirivanov/bigsorter/lines"
)

// Helper to generate a temp file with N random string lines (fixed seed for deterministic benchmarks).
// Note: b is passed as `testing.TB` (interface), NOT `*testing.TB` (pointer to interface).
func generateBenchData(b testing.TB, numLines int) (*os.File, int64) {
	b.Helper()

	f, err := os.CreateTemp("", "bigsorter-bench-input-*")
	if err != nil {
		b.Fatalf("failed to create temp file: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	var totalBytes int64
	buf := make([]byte, 32)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	for i := 0; i < numLines; i++ {
		for j := range buf {
			buf[j] = charset[rng.Intn(len(charset))]
		}
		n, err := f.WriteString(string(buf) + "\n")
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			b.Fatalf("failed to write test data: %v", err)
		}
		totalBytes += int64(n)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		b.Fatalf("failed to rewind file: %v", err)
	}

	return f, totalBytes
}

// Benchmark dataset scaling (10K, 100K, 500K records)
func BenchmarkSort_DatasetSize(b *testing.B) {
	sizes := []int{10_000, 100_000, 500_000}

	for _, numLines := range sizes {
		b.Run(fmt.Sprintf("Records_%d", numLines), func(b *testing.B) {
			inputFile, totalBytes := generateBenchData(b, numLines)
			defer os.Remove(inputFile.Name())
			defer inputFile.Close()

			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = inputFile.Seek(0, io.SeekStart)

				s := &bigsorter.Sorter[string]{
					Serializer: lines.Serializer{},
					Comparator: strings.Compare,
					MaxItems:   20_000,
				}

				if err := s.Sort(inputFile, io.Discard); err != nil {
					b.Fatalf("sort failed: %v", err)
				}
			}
		})
	}
}

// Benchmark tuning Chunk Sizes (MaxItems)
func BenchmarkSort_ChunkSizes(b *testing.B) {
	const numLines = 100_000
	inputFile, totalBytes := generateBenchData(b, numLines)
	defer os.Remove(inputFile.Name())
	defer inputFile.Close()

	chunkSizes := []int{1_000, 10_000, 25_000, 50_000}

	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("MaxItems_%d", chunkSize), func(b *testing.B) {
			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = inputFile.Seek(0, io.SeekStart)

				s := &bigsorter.Sorter[string]{
					Serializer: lines.Serializer{},
					Comparator: strings.Compare,
					MaxItems:   chunkSize,
				}

				if err := s.Sort(inputFile, io.Discard); err != nil {
					b.Fatalf("sort failed: %v", err)
				}
			}
		})
	}
}

// Benchmark Worker Concurrency Levels
func BenchmarkSort_Concurrency(b *testing.B) {
	const numLines = 100_000
	inputFile, totalBytes := generateBenchData(b, numLines)
	defer os.Remove(inputFile.Name())
	defer inputFile.Close()

	workers := []int{1, 2, 4, 8}

	for _, w := range workers {
		b.Run(fmt.Sprintf("Workers_%d", w), func(b *testing.B) {
			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = inputFile.Seek(0, io.SeekStart)

				s := &bigsorter.Sorter[string]{
					Serializer:  lines.Serializer{},
					Comparator:  strings.Compare,
					MaxItems:    10_000,
					Concurrency: w,
				}

				if err := s.Sort(inputFile, io.Discard); err != nil {
					b.Fatalf("sort failed: %v", err)
				}
			}
		})
	}
}
