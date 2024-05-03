package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	saramahealth "github.com/vmyroslav/sarama-health"
)

var (
	topics        = []string{"my-topic"}
	brokers       = []string{"localhost:9092"}
	consumerGroup = "my-consumer-group"
)

func main() {
	ctx := context.Background()

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_1_0
	config.Consumer.Return.Errors = false

	// Create a new consumer group
	group, err := sarama.NewConsumerGroup(brokers, consumerGroup, config)
	if err != nil {
		log.Panicf("Error creating consumer group: %v", err)
	}
	defer func() {
		if err := group.Close(); err != nil {
			log.Panicf("Error closing consumer group: %v", err)
		}
	}()

	healthMonitor, err := saramahealth.NewHealthChecker(saramahealth.Config{
		Brokers:      brokers,
		Topics:       topics,
		SaramaConfig: config,
	})
	if err != nil {
		log.Panicf("Error creating health monitor: %v", err)
	}

	// Consumer group handler
	consumer := Consumer{healthMonitor: healthMonitor}

	// Consume messages
	go func() {
		for {
			err := group.Consume(ctx, topics, &consumer)
			if err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	// Start HTTP server
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		isOk, err := healthMonitor.Healthy(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !isOk {
			http.Error(w, "Not OK", http.StatusServiceUnavailable)
			return
		}

		fmt.Fprintln(w, "OK")
	})

	go func() {
		if err := http.ListenAndServe(":8083", nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready         chan bool
	healthMonitor *saramahealth.HealthChecker
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("session closed: %v\n", ctx.Err())

			consumer.healthMonitor.Release(ctx, claim.Topic(), claim.Partition())

			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			consumer.healthMonitor.Track(ctx, message)

			if string(message.Value) == "fail" { // Simulate a failure
				return fmt.Errorf("error")
			}

			session.MarkMessage(message, "")
		}
	}
}
