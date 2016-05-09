package runner

import (
	"sync"

	"github.com/kuangyh/saw"
)

type TopicID string

type topic struct {
	id          TopicID
	subscribers []saw.Saw
	countVar    saw.VarInt
}

func newTopic(varPrefix string, id TopicID) *topic {
	return &topic{
		id:       id,
		countVar: saw.ReportInt(varPrefix+"."+string(id), "count"),
	}
}

func (t *topic) addSubscriber(saw saw.Saw) {
	t.subscribers = append(t.subscribers, saw)
}

func (t *topic) emit(datum saw.Datum) {
	for _, saw := range t.subscribers {
		saw.Emit(datum)
	}
	t.countVar.Add(1)
}

// Hub is a simple pubsub to allow loosely coupled communication between saws
// Saws can Register with topic(s) it subscribes to, or Publish datum to a topic.
//
// It's a simple local, sync implementation only for better pipeline program structure,
// and it should keep it as it is. parallel, async computing, should be addressed
// by Queues and Pars, implemented by each individual Saw.
type Hub struct {
	varPrefix     string
	mu            sync.Mutex
	topics        map[TopicID]*topic
	deadLetterVar saw.VarInt
}

func NewHub(varPrefix string) *Hub {
	return &Hub{
		varPrefix:     varPrefix,
		topics:        make(map[TopicID]*topic),
		deadLetterVar: saw.ReportInt(varPrefix+".DEAD", "count"),
	}
}

// Register saw that subscribes to a list of Topic
func (hub *Hub) Register(saw saw.Saw, subscribes ...TopicID) {
	hub.mu.Lock()
	defer hub.mu.Unlock()

	for _, topicID := range subscribes {
		topic, ok := hub.topics[topicID]
		if !ok {
			topic = newTopic(hub.varPrefix, topicID)
			hub.topics[topicID] = topic
		}
		topic.addSubscriber(saw)
	}
}

// Publish to topic, resulting in emit to all saws subscirbed in sequence.
// Concurrent calls to Publish() are not synchonized, subscribers is expected
// to handle concurrent Emit()
func (hub *Hub) Publish(id TopicID, datum saw.Datum) {
	topic, ok := hub.topics[id]
	if !ok {
		hub.deadLetterVar.Add(1)
		return
	}
	topic.emit(datum)
}

var GlobalHub = NewHub("global")
