package saramahealth

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"log/slog"
)

type HealthMonitor interface {
	Track(ctx context.Context, msg *sarama.ConsumerMessage) error
	Release(ctx context.Context, topic string, partition int32) error
	Healthy(ctx context.Context) (bool, error)
}

type State struct {
	stateMap map[string]map[int32]int64
}

type HealthCheckerImpl struct {
	topics      []string
	client      sarama.Client
	tracker     *Tracker
	latestState *State
	prevState   *State
	logger      *slog.Logger
}

func NewHealthChecker(cfg Config) (*HealthCheckerImpl, error) {
	client, err := sarama.NewClient(cfg.Brokers, cfg.SaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sarama client")
	}

	return &HealthCheckerImpl{
		client:    client,
		tracker:   NewTracker(),
		topics:    cfg.Topics,
		prevState: nil,
	}, nil
}

func (h *HealthCheckerImpl) Healthy(ctx context.Context) (bool, error) {
	latestState := &State{stateMap: make(map[string]map[int32]int64)}

	for _, topic := range h.topics {
		latestOffset, err := h.getLatestOffset(topic)
		if err != nil {
			return false, err
		}

		latestState.stateMap[topic] = latestOffset
	}

	currentState := h.tracker.CurrentOffsets()
	if h.prevState == nil {
		h.prevState = &State{stateMap: currentState}

		return true, nil
	}

	// check if the current state equals to the latest state
	// return true only if the current state equals to the latest state
	// otherwise go to the next check

	var topicRes bool
	for topic := range currentState {
		for partition := range currentState[topic] {
			if currentState[topic][partition] == latestState.stateMap[topic][partition] {
				topicRes = true
			}
		}
	}

	if topicRes {
		return true, nil
	}

	// check if the current state is greater than the previous state
	// return true only if the current state is greater than the previous state for all topics and partitions
	// otherwise return false

	for topic := range currentState {
		for partition := range currentState[topic] {
			if currentState[topic][partition] <= h.prevState.stateMap[topic][partition] {
				return false, nil
			}
		}
	}

	return true, nil
}

func (h *HealthCheckerImpl) Track(ctx context.Context, msg *sarama.ConsumerMessage) error {
	h.tracker.Track(msg)

	return nil
}

func (h *HealthCheckerImpl) Release(ctx context.Context, topic string, partition int32) error {
	h.tracker.Reset(topic, partition)

	return nil
}

func (h *HealthCheckerImpl) getLatestOffset(topic string) (map[int32]int64, error) {
	var offsets = make(map[int32]int64)

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
