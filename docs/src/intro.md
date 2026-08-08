# Introduction

Welcome to *Concurrent Data Processing in Go*.

Learning Go's concurrency primitives—goroutines, channels, and waitgroups—is
often taught using abstract examples: philosophers dining, workers sleeping, or
simple counters. But to truly master Go, you need to see these patterns applied
to a real-world, high-performance engineering problem.

This book bridges the gap between concurrency theory and production-grade
engineering. We will do this by exploring the architecture of **`etlstream`**, a
disk-backed ETL (Extract, Transform, Load) streaming engine written entirely in
Go[cite: 1].

Originally conceived as an external sorting library named `bigsorter`[cite: 2],
the project evolved to handle the hardest part of massive data processing:
**breaking huge streams into sorted runs on disk**[cite: 1]. By leveraging this
foundation, it provides a framework for processing datasets that are
significantly larger than the available system memory.

### What You Will Learn

By studying the `etlstream` codebase throughout this book, you will learn how
to:

* Design robust **Producer-Consumer pipelines** without leaking goroutines.
* Implement **Fan-Out / Fan-In** architectures to maximize multi-core CPU
  utilization.
* Manage memory allocations tightly to prevent the Go Garbage Collector from
  thrashing.
* Utilize **External Sorting** algorithms to sort and merge terabytes of data
  with a strictly bounded memory footprint.
* Build streaming relational operators (like Joins and GroupBys) on top of
  sorted disk runs[cite: 1].

Whether you are here to understand how to process huge files in Go, or you want
a masterclass in applying concurrency patterns to file I/O, you are in the right
place. Let's dive into the problem space.