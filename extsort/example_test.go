package extsort_test

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/stanimirivanov/etlstream/extsort"
	"github.com/stanimirivanov/etlstream/format/jsonlines"
)

type SolarTelemetry struct {
	PanelID string  `json:"panel_id"`
	OutputW float64 `json:"output_w"`
}

// ExampleSorter_SortContext demonstrates how to parse, sort, and stream
// structured JSON Lines (NDJSON) data using a strongly-typed custom struct.
func ExampleSorter_SortContext() {
	// Unsorted incoming stream of solar panel power readings
	inputJSONL := `{"panel_id":"PNL-03","output_w":320.5}
{"panel_id":"PNL-01","output_w":315.2}
{"panel_id":"PNL-02","output_w":340}
`

	// Sort descending by power output to isolate top-performing panels
	sorter := extsort.Sorter[SolarTelemetry]{
		Serializer: jsonlines.Serializer[SolarTelemetry]{},
		Comparator: func(a, b SolarTelemetry) int {
			if a.OutputW > b.OutputW {
				return -1 // a comes before b
			}
			if a.OutputW < b.OutputW {
				return 1 // b comes before a
			}
			return 0
		},
		MaxItems:    2, // Low chunk size to force K-Way merge in the example
		Concurrency: 2,
	}

	out := &bytes.Buffer{}

	// Execute the external sort pipeline
	err := sorter.SortContext(context.Background(), bytes.NewBufferString(inputJSONL), out)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(out.String())

	// Output:
	// {"panel_id":"PNL-02","output_w":340}
	// {"panel_id":"PNL-03","output_w":320.5}
	// {"panel_id":"PNL-01","output_w":315.2}
}
