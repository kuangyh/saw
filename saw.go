package saw

import (
	"golang.org/x/net/context"
)

type DatumKey string

// Datum is the data item passed between saws. Datum is a key-value pair where
// key must be string and value can by anything. The optional SortOrder
// specifies optimal order when datums with same key get aggregated.
type Datum struct {
	Key   DatumKey
	Value interface{}
}

// Saw is the basic computation unit, it's largely a state machine.
//
// In general, implementation should allow concurrent call to Emit() for good
// parallelism, it can be archive by having a stateless saw (see Transfrom), or
// make it managed by concurrent tables like table.MemTable (see table subpackage).
type Saw interface {
	// Feeds a new data point into Saw.
	Emit(v Datum) error

	// Release all resources and returns final computation result.
	Result(ctx context.Context) (interface{}, error)
}

// Saw can optionally provide Export() interface, it provides a snapshot of its
// current state, which can be later merged to another saw
type ExportSaw interface {
	Export() (interface{}, error)
}

// Saw can merge Export() result of other saws, so that Saws can be aggregated
// or migrated between instances
type MergeSaw interface {
	MergeFrom(other interface{}) error
}

type SawNoResult struct{}

func (snr SawNoResult) Result(ctx context.Context) (interface{}, error) {
	return nil, nil
}
