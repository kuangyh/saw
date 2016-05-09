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

type BatchSpec struct {
	Input           storage.ResourceSpec
	Topic           TopicID
	NumShards       int
	QueueBufferSize int
	// In re-saw, handler are often a table, provide KeyHashFunc allows pre-hash,
	// eliminates unneeded contention.
	KeyHashFunc table.KeyHashFunc
}

type shardRunner struct {
	rc       storage.ResourceSpec
	index    int
	hashFunc table.KeyHashFunc

	topic TopicID
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
		GlobalHub.Publish(runner.topic, datum)
	}, hash)
}

func runInSeq(spec BatchSpec, startInputShard int, numInputShards int, par *Par) {
	for i := startInputShard; i < startInputShard+numInputShards; i++ {
		runner := shardRunner{
			rc:       spec.Input,
			index:    i,
			hashFunc: spec.KeyHashFunc,
			topic:    spec.Topic,
			par:      par,
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
