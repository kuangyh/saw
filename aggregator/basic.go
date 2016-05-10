package aggregator

import (
	"math"

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

type MeanState struct {
	count      Metric
	sum        Metric
	sumOfSqure Metric
}

func (ms *MeanState) Add(metric Metric) {
	ms.count += Metric(1.0)
	ms.sum += metric
	ms.sumOfSqure += metric * metric
}

func (ms *MeanState) Mean() Metric {
	if ms.count == 0 {
		return 0.0
	}
	return ms.sum / ms.count
}

func (ms *MeanState) Stddev() Metric {
	if ms.count <= 1 {
		return 0.0
	}
	return Metric(math.Sqrt(float64((ms.sumOfSqure - ms.Mean()*ms.sum) / (ms.count - 1.0))))
}

func (ms *MeanState) Stderr() Metric {
	if ms.count <= 1 {
		return 0.0
	}
	return ms.Stddev() / Metric(math.Sqrt(float64(ms.count)))
}

type Mean struct {
	state MeanState
}

func (m *Mean) Emit(datum saw.Datum) error {
	m.state.Add(datum.Value.(Metric))
	return nil
}

func (m *Mean) Result() (interface{}, error) {
	return m.state, nil
}
