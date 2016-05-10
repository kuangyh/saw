package aggregator

import (
	"github.com/kuangyh/saw"
	"golang.org/x/net/context"
)

// Sum aggregator saw performs sum aggregation.
type Sum struct {
	Current Metric
}

func (sum *Sum) Emit(datum saw.Datum) error {
	sum.Current += datum.Value.(Metric)
	return nil
}

func (sum *Sum) MergeFrom(other saw.Saw) error {
	sum.Current += other.(*Sum).Current
	return nil
}

func (sum *Sum) Result(ctx context.Context) (interface{}, error) {
	return sum.Current, nil
}
