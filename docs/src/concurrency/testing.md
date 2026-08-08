# Chapter 5: Concurrency Pitfalls & Testing

When you introduce patterns like Fan-Out / Fan-In, your code execution
transitions from deterministic to non-deterministic. The exact order in which
operations occur is now at the mercy of the Go runtime's scheduler and the
operating system's thread management.

This paradigm shift often causes traditional unit tests to fail, even when the
underlying business logic is perfectly sound.

## The Race to the Channel

Consider the following scenario in our Splitter engine:

* The Producer reads two chunks and pushes them to the channel: Chunk A
  (`[delta, alpha]`) and Chunk B (`[charlie, bravo]`).
* We have two workers: Worker 1 picks up Chunk A, and Worker 2 picks up Chunk B.

Because sorting and disk I/O times vary, Worker 2 might finish processing Chunk
B a fraction of a millisecond faster than Worker 1 finishes Chunk A. As a
result, Chunk B's file path is sent to the `fileResCh` channel *before* Chunk
A's file path.

If our unit test asserts that `tempFiles[0]` must contain Chunk A and
`tempFiles[1]` must contain Chunk B, the test will randomly flake and fail.

## Writing Order-Agnostic Tests

To test concurrent code reliably, your assertions must be **order-agnostic**.
Instead of checking array indices, you should verify the *existence* of the
expected outcomes across the entire result set.

Here is how we solved this in `etlstream`'s test suite:

```go
// Define the exact sorted chunks we expect, mapped by their string representation
expectedChunks := map[string]bool{
    "alpha,delta":   false, 
    "bravo,charlie": false, 
    "echo":          false, 
}

// Iterate through the resulting temp files in whatever order they arrived
for _, fpath := range tempFiles {
    content, _ := os.ReadFile(fpath)
    lines := strings.Split(strings.TrimSpace(string(content)), "\n")
    chunkKey := strings.Join(lines, ",")

    // Mark the chunk as found if it matches our expectations
    if _, exists := expectedChunks[chunkKey]; exists {
        expectedChunks[chunkKey] = true
    }
}

// Assert that every expected chunk was discovered
for chunkKey, found := range expectedChunks {
    if !found {
        t.Errorf("expected chunk [%s] was not found", chunkKey)
    }
}
```

By using a map to track state, the test correctly validates the behavior of the
concurrent workers without falsely assuming execution order. The $K$-way merge
phase that follows this step is also inherently order-agnostic—it only requires
that the contents inside each temporary file are sorted, regardless of the order
the files themselves are processed.