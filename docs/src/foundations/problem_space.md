# Chapter 1: The Problem Space

Imagine you are handed a 100 GB file of unsorted customer transactions (CSV or
JSON). Your server has 4 GB of RAM. Your task: Sort the records by `customer_id`
and remove any duplicates.

## The Naive Approach

The standard way most developers learn to parse files in Go is to load the
entire payload into memory using functions like `os.ReadFile()` or by decoding
the whole structure via `json.Unmarshal()`.

If you attempt this with a 100 GB file on a 4 GB machine, the operating system
will swiftly terminate your application with an Out Of Memory (OOM) error.
In-memory data structures cannot scale beyond physical RAM constraints.

## The Streaming Approach

To avoid OOM crashes, we must stream the data. By using `io.Reader` and reading
one record at a time, your memory footprint drops to almost zero.

Streaming is perfect for linear operations like filtering (e.g., "drop
transactions where amount is zero") or mapping (e.g., "convert USD to EUR").
However, **sorting** and **grouping** are fundamentally non-linear operations.
You cannot know if a record is the "first" in a sorted list until you have seen
*every* record.

So, how do we sort data we cannot hold in memory?

## The Solution: External Sorting

The answer is **External Sorting**, a classic algorithm designed exactly for
this scenario. It relies on disk storage to act as an extension of RAM. The
algorithm operates in two primary phases:

### Phase 1: Split and Sort (The Chunking Phase)

Instead of loading the whole file, we read the input stream into memory up to a
predefined limit (e.g., 100,000 records).

1. Read a chunk of records into RAM.
2. Sort that chunk in-memory using a standard fast algorithm (like Quicksort).
3. Flush the sorted chunk to disk as a temporary file.
4. Repeat until the input stream is exhausted.

If we have 100 GB of data, and we chunk it into 1 GB pieces, we will end up with
100 temporary files on disk. Each file is perfectly sorted internally, but the
dataset as a whole is not yet sorted.

### Phase 2: The K-Way Merge

We now have $K$ temporary files on disk. We open a streaming reader for all $K$
files simultaneously.

1. We read the very first record from each of the $K$ files.
2. We place these $K$ records into an in-memory **Min-Heap** data structure.
3. We pop the smallest record from the heap and write it to our final output
   file.
4. Whichever temporary file that smallest record came from, we read its *next*
   record and push it into the heap.

Because each temporary file is already sorted, the Min-Heap always contains the
current smallest candidates across the entire dataset. This guarantees the final
output file is perfectly sorted.

### Algorithmic Complexity

* **Time Complexity:** The total time to process the file is $O (N \log N)$,
  dictated by the sorting of individual chunks and the heap operations during
  the merge.
* **Memory Complexity:** The memory required is strictly bounded to $O (M)$,
  where $M$ is our configured maximum chunk size, completely untethering us from
  the total size of the dataset.

This external sorting mechanism is the exact engine powering `etlstream`. In the
next chapters, we will look at how Go's interfaces map to these concepts, and
how we can use goroutines to parallelize Phase 1 for maximum speed.