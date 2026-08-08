# Chapter 9: Observability & Progress

Data pipelines are often black boxes. When an external sort takes 30 minutes,
operators need to know if the system is actually working or if it has silently
hung. Observability is a first-class requirement for any ETL engine.

To solve this, we introduce **Progress Callbacks**.

## Defining the Progress State

We want to expose metrics without exposing the internal complexity of the
engine. We define a clean `Progress` struct that gets passed back to the user:

```go
type Phase string

const (
    PhaseSplit Phase = "SPLIT"
    PhaseMerge Phase = "MERGE"
)

type Progress struct {
    Phase          Phase
    RecordsRead    int64
    BytesProcessed int64
    TempFilesCount int
}
```

By adding a `ProgressFunc` to our `Sorter` configuration, users can inject
custom logging, update a web UI, or send metrics to Prometheus.

```go
sorter := &extsort.Sorter[User]{
    ProgressFunc: func(p extsort.Progress) {
        log.Printf("[%s] Processed %d records, created %d temp files", 
            p.Phase, p.RecordsRead, p.TempFilesCount)
    },
}
```

## The Synchronization Bottleneck

Implementing this concurrently is trickier than it looks. In the Splitter
engine, multiple worker goroutines are processing chunks simultaneously. If
every worker tries to update a shared `RecordsRead` counter at the same time, we
have a massive race condition.

### Solution 1: Mutexes (The Slow Way)

We could wrap the counter in a `sync.Mutex`. However, locking and unlocking a
mutex thousands of times per second across multiple CPU cores will cause extreme
lock contention, severely degrading the pipeline's throughput.

### Solution 2: Atomic Counters (The Fast Way)

Go's `sync/atomic` package allows us to perform lock-free integer operations at
the hardware level.

```go
import "sync/atomic"

var totalRecords int64

// Inside the worker goroutine:
atomic.AddInt64(&totalRecords, int64(len(chunk)))
```

### Throttling the Callbacks

Even with atomic counters, we shouldn't trigger the user's `ProgressFunc` on
every single record. If the user's function writes to a database or a slow
terminal, it will bottleneck our fast I/O loop.

The best practice is to decouple the state from the reporting. We can use a
`time.Ticker` in a separate background goroutine to read the atomic counters and
fire the `ProgressFunc` exactly once per second, ensuring our reporting is
completely non-blocking.