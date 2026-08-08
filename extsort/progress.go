package extsort

// Phase represents the current execution step of the streaming engine.
type Phase string

const (
	PhaseSplit Phase = "SPLIT"
	PhaseMerge Phase = "MERGE"
)

// Progress contains the current metrics for the streaming operation.
type Progress struct {
	Phase          Phase
	RecordsRead    int64
	TempFilesCount int
}

// ProgressFunc is a callback provided by the user to monitor the pipeline.
type ProgressFunc func(p Progress)
