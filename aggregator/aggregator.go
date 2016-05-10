package aggregator

import (
	"errors"

	"github.com/kuangyh/saw"
)

var ErrNotMergeable = errors.New("saws not compatible to be merged")

// Most aggregators receives (combination) of Metric in Emit()
type Metric float64

// In addition to normal Saw interface, Aggregators should be able to "merge",
// that means it can further aggregate multiple Aggregator saw (may be on different
// instances) into one to provide aggregated result.
type Merger interface {
	MergeFrom(other saw.Saw) error
}
