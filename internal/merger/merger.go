package merger

import (
	stdheap "container/heap"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/stanimirivanov/bigsorter/internal/mergeheap"
	"github.com/stanimirivanov/bigsorter/types"
)

// Options configures the merge phase execution.
type Options[T any] struct {
	Serializer types.Serializer[T]
	Comparator types.Comparator[T]
}

type recordResult[T any] struct {
	record T
	err    error
}

// Merge takes a list of sorted temporary file paths and streams them into output using a Min-Heap.
func Merge[T any](tempFiles []string, output io.Writer, opts Options[T]) error {
	if opts.Serializer == nil {
		return errors.New("serializer is required")
	}
	if opts.Comparator == nil {
		return errors.New("comparator is required")
	}

	if len(tempFiles) == 0 {
		return nil
	}

	files, genChans, err := setupGenerators(tempFiles, opts)
	if err != nil {
		return err
	}
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	rh, err := primeHeap(genChans, opts.Comparator)
	if err != nil {
		return err
	}
	stdheap.Init(rh)

	writer, err := opts.Serializer.CreateWriter(output)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer writer.Close()

	return executeKWayMerge(rh, genChans, writer)
}

func setupGenerators[T any](tempFiles []string, opts Options[T]) ([]*os.File, []<-chan recordResult[T], error) {
	files := make([]*os.File, 0, len(tempFiles))
	genChans := make([]<-chan recordResult[T], 0, len(tempFiles))

	for _, path := range tempFiles {
		f, err := os.Open(path)
		if err != nil {
			return files, nil, fmt.Errorf("failed to open temp file %s: %w", path, err)
		}
		files = append(files, f)

		r, err := opts.Serializer.CreateReader(f)
		if err != nil {
			return files, nil, fmt.Errorf("failed to create reader for %s: %w", path, err)
		}
		genChans = append(genChans, startGenerator(r, 16))
	}
	return files, genChans, nil
}

func startGenerator[T any](r types.Reader[T], bufSize int) <-chan recordResult[T] {
	ch := make(chan recordResult[T], bufSize)
	go func() {
		defer close(ch)
		for {
			rec, err := r.Read()
			if err != nil {
				ch <- recordResult[T]{err: err}
				return
			}
			ch <- recordResult[T]{record: rec}
		}
	}()
	return ch
}

func primeHeap[T any](genChans []<-chan recordResult[T], cmp types.Comparator[T]) (*mergeheap.RecordHeap[T], error) {
	rh := &mergeheap.RecordHeap[T]{
		Items: make([]mergeheap.Item[T], 0, len(genChans)),
		Cmp:   cmp,
	}

	for i, ch := range genChans {
		res := <-ch
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				continue
			}
			return nil, fmt.Errorf("failed to prime heap from temp file %d: %w", i, res.err)
		}
		rh.Items = append(rh.Items, mergeheap.Item[T]{Record: res.record, ReaderIdx: i})
	}
	return rh, nil
}

func executeKWayMerge[T any](rh *mergeheap.RecordHeap[T], genChans []<-chan recordResult[T], writer types.Writer[T]) error {
	for rh.Len() > 0 {
		minItem := stdheap.Pop(rh).(mergeheap.Item[T])

		if err := writer.Write(minItem.Record); err != nil {
			return fmt.Errorf("failed to write record during merge: %w", err)
		}

		res := <-genChans[minItem.ReaderIdx]
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				continue
			}
			return fmt.Errorf("merge read error on temp file %d: %w", minItem.ReaderIdx, res.err)
		}

		stdheap.Push(rh, mergeheap.Item[T]{Record: res.record, ReaderIdx: minItem.ReaderIdx})
	}
	return nil
}
