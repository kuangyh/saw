package runner

import (
	"sync"
	"sync/atomic"

	"github.com/kuangyh/saw"
)

// Default schedule strategy of Par.
const SchedRoundRobin = -1

// Queue emits Datum to internal Saw in sequence.
//
// Queue needed to be created from QueueGroup
type Queue struct {
	dst       saw.Saw
	waitGroup *sync.WaitGroup
	chn       chan saw.Datum
}

func (q *Queue) run() {
	for datum := range q.chn {
		_ = q.dst.Emit(datum)
		q.waitGroup.Done()
	}
}

func (q *Queue) close() {
	close(q.chn)
}

// Schedule a function to run on queue.
func (q *Queue) Sched(datum saw.Datum) {
	q.waitGroup.Add(1)
	q.chn <- datum
}

// Par manages a set of queues, when Sched, it puts task into one of them using
// hash or round-robin
type Par struct {
	round  uint32
	queues []*Queue
}

// Schedule a function to run in one of Par's queues, returns after inserted in
// queue. when hash < 0, schedule select queue by round-robin, otherwise, it
// selects specific queue by hash. hash is just an optimization to minimize
// contention, caller should not relie on queue selecting behavior.
func (par *Par) Sched(datum saw.Datum, hash int) {
	var shard int
	if hash >= 0 {
		shard = hash % len(par.queues)
	} else {
		shard = int(atomic.AddUint32(&par.round, 1)) % len(par.queues)
	}
	par.queues[shard].Sched(datum)
}

// QueueGroup manages a set of queues running colloaborated tasks.
type QueueGroup struct {
	queues    []*Queue
	waitGroup sync.WaitGroup
	mu        sync.Mutex
}

// New creates a queue managed by this QueueGroup.
func (group *QueueGroup) New(dst saw.Saw, bufferSize int) *Queue {
	group.mu.Lock()
	defer group.mu.Unlock()
	queue := &Queue{
		dst:       dst,
		waitGroup: &group.waitGroup,
		chn:       make(chan saw.Datum, bufferSize),
	}
	go queue.run()
	group.queues = append(group.queues, queue)
	return queue
}

// NewPar creates a par with all its queues managed by this QueueGroup
func (group *QueueGroup) NewPar(dst saw.Saw, numShards, bufferSize int) *Par {
	queues := make([]*Queue, numShards)
	for i := 0; i < numShards; i++ {
		queues[i] = group.New(dst, bufferSize)
	}
	return &Par{queues: queues}
}

// Join waits until all pending tasks in queues done, then close and cleanup
// all queues it manages
func (group *QueueGroup) Join() {
	group.mu.Lock()
	defer group.mu.Unlock()

	group.waitGroup.Wait()
	for _, q := range group.queues {
		q.close()
	}
	group.queues = nil
}
