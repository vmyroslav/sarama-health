package saramahealth

import (
	"context"
	"io"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

type HealthMonitor interface {
	Track(ctx context.Context, msg *sarama.ConsumerMessage)
	Release(ctx context.Context, topic string, partition int32)
	Healthy(ctx context.Context) (bool, error)
}

type State struct {
	stateMap map[string]map[int32]int64
}

type HealthChecker struct {
	topics    []string
	client    kafkaClient
	tracker   *tracker
	prevState *State
	logger    *slog.Logger
}

func NewHealthChecker(cfg Config) (*HealthChecker, error) {
	client, err := sarama.NewClient(cfg.Brokers, cfg.SaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sarama client")
	}

	return &HealthChecker{
		client:    client,
		tracker:   newTracker(),
		topics:    cfg.Topics,
		prevState: nil,
		logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}, nil
}

func (h *HealthChecker) Healthy(_ context.Context) (bool, error) {
	// get the latest offset for each topic
	latestStateMap := make(map[string]map[int32]int64)
	for _, topic := range h.topics {
		latestOffset, err := h.getLatestOffset(topic)
		if err != nil {
			return false, err
		}

		latestStateMap[topic] = latestOffset
	}

	currentState := h.tracker.currentOffsets()
	if h.prevState == nil {
		h.prevState = &State{stateMap: currentState}

		return true, nil // return true if this is the first time we check the state
	}

	// check if the current state equals to the latest state
	// return true only if the current state equals to the latest state
	// otherwise go to the next check
	allMatch := true
outer:
	for topic := range currentState {
		for partition := range currentState[topic] {
			if currentState[topic][partition] != latestStateMap[topic][partition] {
				allMatch = false
				break outer
			}
		}
	}

	if allMatch {
		return true, nil // return true if the current state equals to the latest state
	}

	// check if the current state is greater than the previous state
	for topic := range currentState {
		for partition := range currentState[topic] {
			if currentState[topic][partition] <= h.prevState.stateMap[topic][partition] {
				return false, nil
			}
		}
	}

	return true, nil
}

func (h *HealthChecker) Track(_ context.Context, msg *sarama.ConsumerMessage) {
	h.tracker.track(msg)
}

func (h *HealthChecker) Release(_ context.Context, topic string, partition int32) {
	h.tracker.drop(topic, partition)
}

func (h *HealthChecker) SetLogger(l *slog.Logger) {
	h.logger = l
}

func (h *HealthChecker) getLatestOffset(topic string) (map[int32]int64, error) {
	offsets := make(map[int32]int64)

	partitions, err := h.client.Partitions(topic)
	if err != nil {
		return offsets, err
	}

	for _, partition := range partitions {
		offset, err := h.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return offsets, err
		}
		offsets[partition] = offset - 1 // subtract 1 to get the latest offset, not the next offset
	}

	return offsets, nil
}

type kafkaClient interface {
	GetOffset(topic string, partition int32, time int64) (int64, error)
	Partitions(topic string) ([]int32, error)
}
