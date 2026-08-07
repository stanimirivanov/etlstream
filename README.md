# BigSorter (Go)

A Go implementation of an external merge-sort library designed to sort extremely
large files that do not fit into RAM. It does this by splitting files into
smaller chunks, sorting them in memory, and merging them back together.

*Note: The current phase of this project contains the core generic interfaces (
`Reader`, `Writer`, `Serializer`, and `Comparator`) that form the foundation of
the library.*

## Prerequisites

* [Go](https://go.dev/dl/) 1.21 or later (required for Generics and
  `slices.SortFunc`).

## Getting Started

Clone the repository and navigate into the project directory:

```bash
git clone [https://github.com/yourusername/bigsorter.git](https://github.com/yourusername/bigsorter.git)
cd bigsorter
```

## Building the Project

Since bigsorter is a library, there is no executable binary to build. Instead,
you can compile the packages to ensure the code is error-free.

1. Clean up and verify dependencies:
   Ensure your `go.mod` file is up-to-date and all necessary dependencies are
   tracked:

```bash
go mod tidy
```

2. Compile the library:
   Verify that the code compiles successfully across all packages:

```bash
go build ./...
```

3. Format the code:
   Ensure the codebase adheres to standard Go formatting rules:

```bash
go fmt ./...
```