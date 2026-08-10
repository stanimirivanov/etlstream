// Package extsort provides a robust, generic framework for the external
// sorting of unbounded data streams.
//
// It operates in two phases:
//  1. Split: Reads the input stream, chunks the data into memory, sorts
//     each chunk concurrently, and flushes them to temporary files on disk.
//  2. Merge: Initializes a K-way Min-Heap across all temporary files,
//     multiplexing the sorted chunks back into a single output stream.
//
// The package relies on the github.com/stanimirivanov/etlstream/format/types
// interfaces to support arbitrary data formats (e.g., CSV, JSON Lines,
// Fixed-width bytes) via generic serializers.
package extsort
