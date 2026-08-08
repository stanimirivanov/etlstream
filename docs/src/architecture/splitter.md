# Chapter 6: The Splitter Engine

We have discussed the streaming primitives and the concurrency patterns. Now,
let's look at how they come together in the first half of the external sorting
algorithm: the **Splitter Engine**.

The goal of the Splitter is to read an infinite stream of data, break it into
strictly sized chunks, sort those chunks in memory, and persist them to
temporary files.

## Configuring the Boundaries

To prevent out-of-memory (OOM) errors, the engine must know its limits. This is
handled by the `Options[T]` struct passed into the Splitter:

```go
type Options[T any] struct {
    Serializer  types.Serializer[T]
    Comparator  types.Comparator[T]
    MaxItems    int
    Concurrency int
    TempDir     string
}
```

The most critical parameter here is `MaxItems`. This defines exactly how many
records are allowed to reside in RAM per worker goroutine. If `MaxItems` is set
to 100,000, and you have a `Concurrency` of 4, the absolute maximum memory
footprint of the Splitter engine will be 400,000 records, regardless of whether
the total dataset contains millions or billions of rows.

## The Normalization Step

What happens if a user forgets to configure these limits? A robust engine should
protect itself with sensible defaults.

```go
{{#include ../../../extsort/internal/splitter/splitter.go:136:143}} 
```

By defaulting to 100,000 items and using `runtime.NumCPU()`, the engine ensures
that it runs optimally out-of-the-box on the host machine without accidentally
attempting to load an entire terabyte file into memory.

## Generating Temporary Files

When a worker receives a chunk, it sorts it using Go's highly optimized
slices.SortFunc and the provided Comparator. Once sorted, it must be flushed to
disk.

```go
{{#include ../../../extsort/internal/splitter/splitter.go:88:105}}
```

Notice how we use `os.CreateTemp` to generate unique file names. Because we are
in a concurrent environment, multiple workers are writing to the disk at the
exact same millisecond. Using a secure temporary file generation strategy
prevents naming collisions and race conditions on the filesystem.

Once the entire stream has been chunked, sorted, and written to disk, the
Splitter returns the list of file paths. The dataset is now ready for the K-Way
Merge.