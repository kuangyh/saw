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

func (hub *Hub) Publish(id TopicID, datum saw.Datum) {
	topic, ok := hub.topics[id]
	if !ok {
		hub.deadLetterVar.Add(1)
		return
	}
	topic.emit(datum)
}

var GlobalHub = NewHub("global")
