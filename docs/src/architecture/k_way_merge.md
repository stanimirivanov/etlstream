# Chapter 7: The K-Way Merge Engine

The Splitter leaves us with multiple temporary files on disk, each internally
sorted. The final challenge is merging these files into a single, globally
sorted output stream without loading them all into memory.

This is accomplished using a **K-Way Merge** algorithm powered by a Min-Heap
data structure.

## The Min-Heap

A Min-Heap is a specialized tree-based data structure that always keeps the
smallest element at the root. In Go, we can turn any slice into a Heap by
implementing the `heap.Interface` from the `container/heap` standard library
package.

To merge *K* files (where *K* is the number of temporary files), we don't load
the files themselves into the heap. Instead, we load exactly **one record** from
each file.

```go
// A conceptual look at the items stored in our Heap
type MergeItem[T any] struct {
    Record T           // The actual data record
    Reader types.Reader[T] // The stream this record came from
}
```

## The Merge Loop

The algorithm operates in a continuous loop until all temporary files are
exhausted:

1. **Initialization**: Open a `Reader` for every temporary file. Read the very
   first record from each file and push it into the Min-Heap. If we have 50
   temporary files, our heap size is exactly 50.

2. **Pop the Minimum**: Ask the Heap for the smallest item. Because it is a
   Min-Heap, this operation is highly efficient.

3. **Write to Output**: Take the record from that smallest item and write it
   directly to the final output file.

4. **Advance the Stream**: Using the Reader attached to that item, read the next
   record from the same temporary file.

5. **Push to Heap**: Push this new record into the Min-Heap.

6. **Repeat**: Continue popping and pushing until every reader returns io.EOF.

## Why This is Powerful

This approach has a profound impact on memory usage. The memory footprint during
the merge phase is proportional to K (the number of temporary files), not the
size of the dataset.

If you are sorting 1 billion records and the Splitter created 100 temporary
files, the K-Way Merge engine only holds 100 records in memory at any given
time.

```go
// Example of Go's heap popping and pushing mechanics
// This ensures we always find the next smallest item efficiently
smallestItem := heap.Pop(mergeHeap).(*MergeItem[T])
err := outputWriter.Write(smallestItem.Record)

nextRecord, err := smallestItem.Reader.Read()
if err == nil {
    // If the file isn't empty, push the next item into the heap
    heap.Push(mergeHeap, &MergeItem[T]{
        Record: nextRecord,
        Reader: smallestItem.Reader,
    })
}
```

By chaining the Splitter and the K-Way Merge together, `etlstream` achieves
infinite scalability for data sorting, restricted only by the amount of free
disk space available to hold the temporary files.