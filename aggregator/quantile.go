package aggregator

import (
	"sort"

	"github.com/kuangyh/saw"
	"golang.org/x/net/context"
)

// QuantileState implements online quantile calculation with O(LogN) space compleixty,
// based on http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf
//
// User should use NewQuantile() to create a QuantileSaw intead of using QuantileState
// directly, unless user wants to use quantile calculation as part of their aggregation
// algorithm.
type QuantileState struct {
	bufferSize   int
	leaf         []Metric
	sampleStack  [][]Metric
	min          Metric
	max          Metric
	hasMetric    bool
	collapseFlip int
}

func NewQuantileState(bufferSize int) *QuantileState {
	return &QuantileState{
		bufferSize: bufferSize,
		leaf:       make([]Metric, 0, bufferSize*2),
	}
}

type metricSort []Metric

func (ms metricSort) Len() int           { return len(ms) }
func (ms metricSort) Less(i, j int) bool { return ms[i] < ms[j] }
func (ms metricSort) Swap(i, j int)      { t := ms[i]; ms[i] = ms[j]; ms[j] = t }

func (qs *QuantileState) AddMetric(metric Metric) {
	qs.leaf = append(qs.leaf, metric)
	if len(qs.leaf) == qs.bufferSize*2 {
		left := qs.leaf[:qs.bufferSize]
		right := qs.leaf[qs.bufferSize:]
		sort.Sort(metricSort(left))
		sort.Sort(metricSort(right))

		leafCollapsed := qs.collapse(left, right)
		qs.leaf = qs.leaf[:0]
		qs.mergeSampleStack(0, leafCollapsed)
	}
	if qs.hasMetric {
		if metric < qs.min {
			qs.min = metric
		}
		if metric > qs.max {
			qs.max = metric
		}
	} else {
		qs.min = metric
		qs.max = metric
	}
	qs.hasMetric = true
}

func (qs *QuantileState) collapse(left, right []Metric) []Metric {
	var leftIdx, rightIdx int
	merged := make([]Metric, qs.bufferSize)
	for leftIdx+rightIdx < qs.bufferSize*2 {
		var pick Metric
		if leftIdx >= len(left) {
			pick = right[rightIdx]
			rightIdx++
		} else if rightIdx >= len(right) {
			pick = left[leftIdx]
			leftIdx++
		} else if left[leftIdx] < right[rightIdx] {
			pick = left[leftIdx]
			leftIdx++
		} else {
			pick = right[rightIdx]
			rightIdx++
		}
		// 0.5 sample
		if (leftIdx+rightIdx)%2 == qs.collapseFlip {
			merged[(leftIdx+rightIdx)/2] = pick
		}
	}
	qs.collapseFlip = 1 - qs.collapseFlip
	return merged
}

func (qs *QuantileState) mergeSampleStack(startLevel int, buf []Metric) {
	var level int
	for level = startLevel; level < len(qs.sampleStack); level++ {
		if qs.sampleStack[level] == nil {
			qs.sampleStack[level] = buf
			return
		}
		buf = qs.collapse(qs.sampleStack[level], buf)
		qs.sampleStack[level] = nil
	}
	// Come to top, add new level(s), note that when doing mergeFrom, startLevel
	// can already > len(qs.sampleStack), the loop is neccesary
	for len(qs.sampleStack) < level {
		qs.sampleStack = append(qs.sampleStack, nil)
	}
	qs.sampleStack[level] = buf
}

func (qs *QuantileState) MergeFrom(other *QuantileState) error {
	if qs.bufferSize != other.bufferSize {
		return ErrNotMergeable
	}
	if !other.hasMetric {
		return nil
	}
	if qs.hasMetric {
		if other.min < qs.min {
			qs.min = other.min
		}
		if other.max > qs.max {
			qs.max = other.max
		}
	} else {
		qs.min = other.min
		qs.max = other.max
	}
	for i := len(other.sampleStack) - 1; i >= 0; i-- {
		qs.mergeSampleStack(i, other.sampleStack[i])
	}
	for _, metric := range other.leaf {
		qs.AddMetric(metric)
	}
	return nil
}

type weightedMetric struct {
	metric Metric
	weight int
}

type weightedMetricSort []weightedMetric

func (ws weightedMetricSort) Len() int           { return len(ws) }
func (ws weightedMetricSort) Less(i, j int) bool { return ws[i].metric < ws[j].metric }
func (ws weightedMetricSort) Swap(i, j int)      { t := ws[i]; ws[i] = ws[j]; ws[j] = t }

func (qs *QuantileState) Result() Quantile {
	var queryBuf []weightedMetric
	total := 0
	for _, leafMetric := range qs.leaf {
		queryBuf = append(queryBuf, weightedMetric{leafMetric, 1})
		total++
	}
	for level, sampleBuf := range qs.sampleStack {
		weight := 1 << uint(level+2)
		for _, metric := range sampleBuf {
			queryBuf = append(queryBuf, weightedMetric{metric: metric, weight: weight})
			total += weight
		}
	}
	sort.Sort(weightedMetricSort(queryBuf))
	return Quantile{
		total:    total,
		min:      qs.min,
		max:      qs.max,
		queryBuf: queryBuf,
	}
}

// Quantile is a snapshot of QuantileState that provides query interface for getting
// quantile metrics.
type Quantile struct {
	total    int
	min      Metric
	max      Metric
	queryBuf []weightedMetric
}

// Get quantile of point ratio between 0..1. min value returned for ratio=0,
// max value for ratio=1, estimated quantile for ratios in between.
func (q *Quantile) At(ratio float64) Metric {
	if ratio <= 0.0 {
		return q.min
	}
	if ratio >= 1.0 {
		return q.max
	}
	targetWeight := float64(q.total) * ratio
	var currWeight float64
	for _, wm := range q.queryBuf {
		currWeight += float64(wm.weight)
		if currWeight >= targetWeight {
			return wm.metric
		}
	}
	return q.max
}

// Get quantile list for buckets.
//
// when numBuckets <= 1, only min and max are returned, otherwise, a slice with
// length numBucket+1 returns, with min, quantile for each bucket and max.
//
// Example: numBuckets = 2 returns [min, median, max]
func (q *Quantile) Get(numBuckets int) []Metric {
	if numBuckets <= 1 {
		return []Metric{q.min, q.max}
	}
	output := make([]Metric, numBuckets+1)
	output[0] = q.min
	output[numBuckets] = q.max
	step := float64(q.total) / float64(numBuckets)
	var currWeight float64
	targetWeight := step

	idx := 1
	for _, wm := range q.queryBuf {
		if idx >= numBuckets {
			break
		}
		currWeight += float64(wm.weight)
		for currWeight >= targetWeight && idx < numBuckets {
			output[idx] = wm.metric
			idx++
			targetWeight += step
		}
	}
	return output
}

// QuantileSaw wraps QuantileState to provide Saw interface.
type QuantileSaw struct {
	state *QuantileState
}

func (s *QuantileSaw) Emit(datum saw.Datum) error {
	s.state.AddMetric(datum.Value.(Metric))
	return nil
}

// Returns Quantile
func (s *QuantileSaw) Result(ctx context.Context) (interface{}, error) {
	return s.state.Result(), nil
}

func (s *QuantileSaw) MergeFrom(other saw.Saw) error {
	return s.state.MergeFrom(other.(*QuantileSaw).state)
}

// Creates a new QuantileSaw, desiresNumBuckets and samplesPerBucket determines
// bufferSize of QuantileState.
//
// desireNumBuckets is the # buckets you will query against saw result,
// samplesPerBucket is minimum # of samples you want to use to calculate a single
// quantile bucket, 10 is enough for most cases.
//
// Plan memory usage with formula:
// (log(# samples) + 1) * desireNumBuckets * samplesPerBucket.
func NewQuantile(desireNumBuckets int, samplesPerBucket int) *QuantileSaw {
	bufferSize := int(desireNumBuckets * samplesPerBucket)
	return &QuantileSaw{state: NewQuantileState(bufferSize)}
}
