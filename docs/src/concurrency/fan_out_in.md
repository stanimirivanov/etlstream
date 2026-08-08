# Chapter 4: Fan-Out / Fan-In Engine

In the previous chapter, we built a Producer that continuously reads records and
pushes chunks into a channel. If we only had one consumer reading from that
channel, we wouldn't be utilizing modern multi-core processors effectively.

To maximize throughput, we use a concurrency pattern known as **Fan-Out /
Fan-In**.

## The Fan-Out (Workers)

"Fan-Out" refers to starting multiple worker goroutines that all read from the
exact same channel. Go channels are concurrency-safe; when multiple goroutines
listen to a single channel, the Go runtime automatically distributes the
incoming messages among the workers round-robin style, ensuring no two workers
process the same chunk.

Here is how `etlstream` implements the Fan-Out phase in the Splitter engine:

```go
{{#include ../../../extsort/internal/splitter/splitter.go:76:86}}
```

### Key Mechanisms:

1. **Concurrency Configuration**: We pass a `concurrency` integer (typically
   defaulting to the number of logical CPUs on the machine). This dictates
   exactly how many worker goroutines we spawn.

2. `sync.WaitGroup`: We pass a pointer to a `sync.WaitGroup` and call
   `wg.Add(1)`
   before starting each goroutine. This acts as a thread-safe counter, allowing
   the main program to know exactly how many workers are currently running.

3. **The `range` Loop**: The workers use `for chunk := range chunksCh`. This
   loop will continuously pull chunks from the channel and process them. It only
   terminates when the Producer closes the `chunksCh` channel and the channel is
   completely empty.

## The Fan-In (Collector)

Once the workers sort their chunks and write them to temporary files, they need
a way to report the generated file paths back to the main thread. This brings us
to the "Fan-In" phase: multiplexing the results from multiple workers into a
single channel.

All workers write their results to a single `fileResCh`. But who closes this
channel? We cannot close it inside the worker, because other workers might still
be running. We must wait until all workers have finished.

```go
{{#include ../../../extsort/internal/splitter/splitter.go:107:134}}
```

### The Collector Pattern:

1. **Background Waiting**: We spawn a dedicated, lightweight goroutine solely
   responsible for calling `wg.Wait()`. Once the WaitGroup counter hits zero
   (meaning all workers have returned), this goroutine closes the `fileResCh`.

2. **Result Aggregation**: The main thread ranges over `fileResCh`, appending
   the paths of successfully created temporary files to a slice.

3. **Graceful Cleanup**: If any error occurred during reading or writing, the
   engine iterates through the `tempFiles` slice and deletes the partially
   created files from disk before returning the error to the user, ensuring we
   don't leak temporary storage.