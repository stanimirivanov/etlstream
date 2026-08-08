# Chapter 2: Go Streaming Primitives

To process massive datasets efficiently, we must strictly control how data moves
from disk into our application's memory and back out again.

In the Go standard library, data streams are fundamentally represented by two
interfaces: `io.Reader` and `io.Writer`. These interfaces are elegant and
ubiquitous, but they operate entirely on raw bytes (`[]byte`).

When building an ETL pipeline, we don't want to think about bytes; we want to
think about **Records**—whether those are parsed CSV rows, JSON objects, or
fixed-width text structs.

## Bridging Bytes and Types

To bridge the gap between raw bytes and structured records, `etlstream` defines
generic streaming interfaces in the `format/types` package[cite: 2].

```go
// Reader represents a stream of typed records.
type Reader[T any] interface {
    // Read returns the next record in the stream.
    // It returns io.EOF when the stream is exhausted.
    Read() (T, error)
}

// Writer represents a destination for typed records.
type Writer[T any] interface {
    // Write persists a single record to the stream.
    Write(record T) error
    // Close flushes any internal buffers and closes the underlying resource.
    Close() error
}
```

## The Role of the Serializer

Because our engine utilizes External Sorting (writing temporary chunks to disk
during Phase 1), it needs to know how to instantiate new `Reader` and `Writer`
instances on the fly for these temporary files.

This is handled by the `Serializer` interface:

```go
type Serializer[T any] interface {
CreateReader(r io.Reader) (Reader[T], error)
CreateWriter(w io.Writer) (Writer[T], error)
}
```

Whenever the internal engine creates a new temporary file on disk, it passes the
raw `*os.File` (which implements `io.Writer`) to CreateWriter. This returns a
typed
`Writer[T]` ready to accept our Go structs.

## Memory Allocation Considerations

A critical aspect of implementing `Reader[T]` for high-performance streaming is
memory management. If a file contains 100 million records, returning a brand new
struct allocation from `Read()` on every single call will put massive pressure
on the Go Garbage Collector, slowing down the entire pipeline.

High-performance implementations of `Reader[T]` often reuse memory internally,
deserializing new data into existing struct fields. This ensures that the memory
footprint remains flat, regardless of whether the stream has processed a
thousand records or a billion.

With our primitives established, we can now look at how to move these records
concurrently.