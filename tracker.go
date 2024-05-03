package saramahealth

import (
	"github.com/IBM/sarama"
	"sync"
)

type Tracker struct {
	topicPartitionOffsets map[string]map[int32]int64 //todo: change to sync.Map
	mu                    sync.RWMutex
}

func NewTracker() *Tracker {
	top := make(map[string]map[int32]int64)

	return &Tracker{topicPartitionOffsets: top}
}

func (t *Tracker) Track(m *sarama.ConsumerMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.topicPartitionOffsets[m.Topic]; !ok {
		t.topicPartitionOffsets[m.Topic] = make(map[int32]int64)
	}

	t.topicPartitionOffsets[m.Topic][m.Partition] = m.Offset
}

func (t *Tracker) CurrentOffsets() map[string]map[int32]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.topicPartitionOffsets
}

func (t *Tracker) Reset(topic string, partition int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.topicPartitionOffsets[topic]; !ok {
		t.topicPartitionOffsets[topic] = make(map[int32]int64)
	}

	t.topicPartitionOffsets[topic][partition] = 0
}
