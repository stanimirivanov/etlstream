# Chapter 3: The Pipeline Pattern

The first phase of our streaming engine is the **Splitter**. Its job is to read
an infinite stream of records, group them into finite "chunks," sort those
chunks, and write them to temporary files[cite: 2].

If we do this sequentially—read a chunk, sort it, write it, and *then* read the
next one—we are wasting system resources. The CPU sits idle while we read from
disk, and the disk sits idle while we sort in memory.

To maximize throughput, we must parallelize this using Go's concurrency
primitives. We achieve this using a **Producer-Consumer Pipeline**.

## The Producer

The Producer's sole responsibility is to extract records from the input stream,
assemble them into memory-bounded arrays (chunks), and push those chunks into a
Go channel.

We can pull the exact implementation of our producer directly from
`extsort/internal/splitter/splitter.go`[cite: 2]:

```go
{{#include ../../../extsort/internal/splitter/splitter.go:50:66}}
```

## Analyzing the Producer

Let's break down the critical concurrency patterns demonstrated in this
function:

1. **Channel Ownership**: In Go, the goroutine that writes to a channel should
   be the one responsible for closing it. By using `defer close (chunksCh)` and `defer close
(readErrCh)`, we guarantee that no matter how this function exits (success, EOF,
   or unexpected error), the downstream consumers will receive a signal that no
   more data is coming.

2. **Error Handling in Goroutines**: A goroutine cannot simply return an error
   to the function that spawned it. Instead, we use a dedicated `readErrCh`
   channel to propagate the error back to the main coordinating thread. Notice
   that we intentionally ignore `io.EOF`—that is simply the end of the stream,
   not a failure.

3. **Non-Blocking Execution**: As long as there is space in the `chunksCh`
   buffer,
   `chunksCh <- chunk` will not block. The producer can immediately move on to
   reading the next chunk while the consumer is busy sorting the previous one.

## The Consumer (s)

While the Producer is busy reading, we need workers (Consumers) to take those
chunks, sort them, and write them to disk. In the next chapter, we will look at
how we spin up a dynamically sized pool of workers to process these chunks
concurrently using the Fan-Out / Fan-In pattern.