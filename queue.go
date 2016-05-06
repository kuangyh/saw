package saw

import (
	"sync"
	"sync/atomic"
)

// Queue runs function scheduled in a seperate gorountine. Normally you don't
// need to explicitly create or use Queue, Par / Tables implicitly create queues
// and schedule user-defined Saws to some of them.
type Queue struct {
	// TODO(yuheng): potential contetion hotspot, current benchmark doesn't show
	// problem. Find other solution if it's actually troublesome.
	waitGroup *sync.WaitGroup
	chn       chan func()
}

func (q *Queue) run() {
	for f := range q.chn {
		f()
		q.waitGroup.Done()
	}
}

func (q *Queue) close() {
	close(q.chn)
}

// Schedule a function to run on queue.
func (q *Queue) Sched(f func()) {
	q.waitGroup.Add(1)
	q.chn <- f
}

// Par manages a set of queues, when Sched, it puts task into one of them using
// round-robin
type Par struct {
	round  uint32
	queues []*Queue
}

func (par *Par) Sched(f func()) {
	shard := int(atomic.AddUint32(&par.round, 1)) % len(par.queues)
	par.queues[shard].Sched(f)
}

// QueueGroup manages a set of queues running colloaborated tasks.
type QueueGroup struct {
	queues    []*Queue
	waitGroup sync.WaitGroup
	mu        sync.Mutex
}

// New creates a queue managed by this QueueGroup.
func (group *QueueGroup) New(bufferSize int) *Queue {
	group.mu.Lock()
	defer group.mu.Unlock()
	queue := &Queue{
		waitGroup: &group.waitGroup,
		chn:       make(chan func(), bufferSize),
	}
	go queue.run()
	group.queues = append(group.queues, queue)
	return queue
}

// NewPar creates a par with all its queues managed by this QueueGroup
func (group *QueueGroup) NewPar(numShards, bufferSize int) *Par {
	queues := make([]*Queue, numShards)
	for i := 0; i < numShards; i++ {
		queues[i] = group.New(bufferSize)
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
