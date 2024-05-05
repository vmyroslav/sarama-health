package saramahealth

import (
	"sync"

	"github.com/IBM/sarama"
)

type tracker struct {
	topicPartitionOffsets map[string]map[int32]int64
	mu                    sync.RWMutex
}

func newTracker() *tracker {
	return &tracker{topicPartitionOffsets: make(map[string]map[int32]int64)}
}

func (t *tracker) track(m *sarama.ConsumerMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.topicPartitionOffsets[m.Topic]; !ok {
		t.topicPartitionOffsets[m.Topic] = make(map[int32]int64)
	}

	t.topicPartitionOffsets[m.Topic][m.Partition] = m.Offset
}

func (t *tracker) currentOffsets() map[string]map[int32]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.topicPartitionOffsets
}

func (t *tracker) drop(topic string, partition int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.topicPartitionOffsets[topic], partition)
}
