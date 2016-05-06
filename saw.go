package saw

import (
	"golang.org/x/net/context"
)

type DatumKey string

// Datum is the data item passed between saws. Datum is a key-value pair where
// key must be string and value can by anything. The optional SortOrder
// specifies optimal order when datums with same key get aggregated.
type Datum struct {
	Key       DatumKey
	Value     interface{}
	SortOrder uint64
}

// Saw is the basic computation unit, it's largely a state machine. Runtime
// garunteees that all its methods are called in serial (not neccessary on same
// thread / goroutine).
type Saw interface {
	// Feeds a new data point into Saw, implementation should allow concurrent call
	// for this method, or use Queue / Par / Table to manage concurrency.
	Emit(v Datum) error

	// Returning computation result based on current state, this can take long if
	// Saw does reduce computation or wait for other saws, so a context with timeout
	// is passed in for early cancelation
	//
	// Implementation should expect same concurrency assumption as Emit()
	Result(ctx context.Context) (interface{}, error)
}
