package runner

import (
	"io"
	"log"
	"math"
	"sync"

	"github.com/kuangyh/saw"
	"github.com/kuangyh/saw/storage"
	"github.com/kuangyh/saw/table"
	"golang.org/x/net/context"
)

// Speicify one data source of batch computation.
type BatchSpec struct {
	// Reads data from here
	Input storage.ResourceSpec
	// Optional, decode input instead of passing []byte
	InputValueDecoder saw.ValueDecoder
	// Then data will be publish to this topic
	Topic saw.TopicID
	// Use NumShards queues to call subscribers in parallel, it makes no sense
	// if subscriber doesn't handle concurrent Emit().
	// NumShards can be equal, smaller or larger than Input.NumShards, implementation
	// make best effort to keep efficiency.
	NumShards       int
	QueueBufferSize int
	// In re-saw, handler are often a table, provide KeyHashFunc allows pre-hash,
	// eliminates unneeded contention.
	KeyHashFunc table.KeyHashFunc
}

type shardRunner struct {
	rc           storage.ResourceSpec
	index        int
	hashFunc     table.KeyHashFunc
	valueDecoder saw.ValueDecoder

	topic saw.TopicID
	par   *Par
}

func (runner *shardRunner) run() {
	reader, err := runner.rc.DatumReader(context.Background(), runner.index)
	if err != nil {
		log.Printf(
			"Unable to open DatumReader for %v, shard=%d, err=%v",
			runner.rc, runner.index, err)
		return
	}
	defer reader.Close()

	var datum saw.Datum
	for {
		datum, err = reader.ReadDatum()
		if err != nil {
			break
		}
		hash := -1
		if runner.hashFunc != nil {
			hash = runner.hashFunc(datum.Key)
		}
		runner.sched(datum, hash)
	}
	if err != io.EOF {
		log.Printf("DatumReader error for %v, shard=%d, err=%v", runner.rc, runner.index, err)
	}
}

func (runner *shardRunner) sched(datum saw.Datum, hash int) {
	runner.par.Sched(func() {
		if runner.valueDecoder != nil {
			decodedValue, err := runner.valueDecoder.DecodeValue(datum.Value.([]byte))
			if err != nil {
				return
			}
			datum.Value = decodedValue
		}
		saw.GlobalHub.Publish(runner.topic, datum)
	}, hash)
}

func runInSeq(spec BatchSpec, startInputShard int, numInputShards int, par *Par) {
	for i := startInputShard; i < startInputShard+numInputShards; i++ {
		runner := shardRunner{
			rc:           spec.Input,
			index:        i,
			hashFunc:     spec.KeyHashFunc,
			valueDecoder: spec.InputValueDecoder,
			topic:        spec.Topic,
			par:          par,
		}
		runner.run()
	}
}

func runSingleBatch(spec BatchSpec, queueGroup *QueueGroup) {
	var numInputShards int
	if spec.Input.Sharded() {
		numInputShards = spec.Input.NumShards
	} else {
		numInputShards = 1
	}
	var wg sync.WaitGroup
	if spec.NumShards < numInputShards {
		// 1 runner vs. multiple input
		var remain float64 = 0.0
		var inputsPerShard = float64(numInputShards) / float64(spec.NumShards)
		currInputShard := 0
		for i := 0; i < spec.NumShards; i++ {
			next := remain + inputsPerShard
			numInputs := int(math.Floor(next + 0.5))
			remain = next - float64(numInputs)
			wg.Add(1)
			go func(startInputShard, numInputShards int) {
				log.Printf(
					"Start runner input=%v, topic=%v, shard=%d:%d, queuePerShard=1",
					spec.Input, spec.Topic, startInputShard, startInputShard+numInputShards-1)
				par := queueGroup.NewPar(1, spec.QueueBufferSize)
				runInSeq(spec, startInputShard, numInputShards, par)
				wg.Done()
			}(currInputShard, numInputs)
			currInputShard += numInputs
		}
	} else {
		// 1 runner vs. 1 input vs. multiple queue
		var remain float64 = 0.0
		var queuesPerShard = float64(spec.NumShards) / float64(numInputShards)
		for i := 0; i < numInputShards; i++ {
			next := remain + queuesPerShard
			numQueues := int(math.Floor(next + 0.5))
			remain = next - float64(numQueues)
			wg.Add(1)
			go func(shardIdx, numQueues int) {
				log.Printf(
					"Start runner input=%v, topic=%v, shard=%d:%d, queuePerShard=%d",
					spec.Input, spec.Topic, shardIdx, shardIdx, numQueues)
				par := queueGroup.NewPar(numQueues, spec.QueueBufferSize)
				runInSeq(spec, shardIdx, 1, par)
				wg.Done()
			}(i, numQueues)
		}
	}
	wg.Wait()
}

// Run batch job, ingest all source data in parallel, returns after all data
// are published to specified topic.
//
// It doesn't guaranttee Saw computation finishes --- in batch program, you must
// call Result() for top level saws to make sure it fnishes computation and stores
// data.
func RunBatch(source ...BatchSpec) {
	var queueGroup QueueGroup
	var wg sync.WaitGroup

	for _, spec := range source {
		wg.Add(1)
		go func(spec BatchSpec) {
			runSingleBatch(spec, &queueGroup)
			wg.Done()
		}(spec)
	}
	wg.Wait()
	queueGroup.Join()
}
