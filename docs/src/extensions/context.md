# Chapter 8: Context & Graceful Teardown

When processing multi-gigabyte files, operations can take minutes or even hours.
In a production environment, you cannot afford to have a runaway process that
cannot be stopped. If a Kubernetes pod is shutting down, or a user clicks
"Cancel" on a web dashboard, your ETL engine must halt gracefully.

In Go, this is universally handled using the `context.Context` package.

## The Context Propagation Pattern

To make `etlstream` cancellable, we introduce a new method to our API:
`SortContext`.

```go
// SortContext performs the external sort but listens for cancellation signals.
func (s *Sorter[T]) SortContext(ctx context.Context, input io.Reader, output io.Writer) error
```

The challenge with external sorting is that the work is deeply nested. The main
thread spawns a Splitter engine, which spawns workers, which write to disk.
Then, it spawns a K-Way Merge engine. The `ctx` must be propagated down into the
deepest loops of these engines.

## Polling for Cancellation

Go's context does not magically interrupt running goroutines. It is cooperative.
The goroutines must actively check if the context has been canceled.

Inside the Splitter's chunk-reading loop and the K-Way Merge's heap-popping
loop, we must periodically poll the context's `Done()` channel:

```go
// Inside a long-running data processing loop
for {
    // 1. Check for cancellation
    select {
    case <-ctx.Done():
        return ctx.Err() // e.g., context.Canceled
    default:
        // Continue processing
    }

    // 2. Read, sort, or merge the next record...
}
```

### The Performance Cost of Select

Calling `select` on every single record in a 100-million record dataset will
introduce significant overhead. The Go runtime has to lock and check channel
states repeatedly.

To optimize this, high-performance engines often implement **batch polling**.
Instead of checking the context on every iteration, we only check it every $N$
records (e.g., every 10,000 iterations). This keeps the CPU focused on data
processing while still responding to a cancellation signal within a few
milliseconds.

## Graceful Cleanup

When a context is canceled, it is not enough to just return an error. The engine
is likely in the middle of writing temporary files to disk. A robust graceful
teardown must catch the `context.Canceled` error, bubble it up to the main
orchestrator, and trigger the defer block that sweeps the filesystem and deletes
any orphaned temporary files.