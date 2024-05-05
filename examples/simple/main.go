package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	saramahealth "github.com/vmyroslav/sarama-health"
)

var (
	topics        = []string{"health-check-topic"}
	brokers       = []string{"localhost:9092"}
	consumerGroup = "my-consumer-group"
	httpPort      = 8080
)

func main() {
	ctx := context.Background()

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_1_0
	config.Consumer.Return.Errors = false
	config.Consumer.MaxProcessingTime = 20 * time.Millisecond

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
	}))

	topic := os.Getenv("SARAMA_DEMO_TOPIC")
	if topic != "" {
		topics = []string{topic}
	}

	broker := os.Getenv("SARAMA_DEMO_BROKER")
	if broker != "" {
		brokers = []string{broker}
	}

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

	healthMonitor.SetLogger(logger)

	// Consumer group handler
	consumer := Consumer{healthMonitor: healthMonitor, l: logger}

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
		logger.InfoContext(ctx, "Health check request received")
		isOk, err := healthMonitor.Healthy(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			logger.ErrorContext(ctx, "Error checking health", err)

			return
		}

		if !isOk {
			http.Error(w, "Not OK", http.StatusServiceUnavailable)

			logger.ErrorContext(ctx, "Health check failed")
			return
		}

		_, _ = fmt.Fprintln(w, "OK")
	})

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
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
	l             *slog.Logger
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.l.Debug("Consumer group is ready!...")

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	c.l.Debug("Consumer closing down!...")

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for {
		select {
		case <-ctx.Done():
			c.l.DebugContext(
				ctx,
				"Session closed",
				"topic",
				claim.Topic(),
				"partition",
				claim.Partition(),
				"error",
				ctx.Err(),
			)

			c.healthMonitor.Release(ctx, claim.Topic(), claim.Partition())

			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.l.DebugContext(
				ctx,
				"Message claimed",
				"topic",
				message.Topic,
				"partition",
				message.Partition,
				"offset",
				message.Offset,
			)

			c.healthMonitor.Track(ctx, message)

			// Read the file
			data, err := os.ReadFile("./examples/simple/failure.txt")
			if err != nil {
				log.Println("Error reading file:", err)
				continue
			}

			// Check if the file contains 'true'
			if strings.TrimSpace(string(data)) == "fail" {
				log.Println("Simulating a failure")
				time.Sleep(40 * time.Second)
			}

			if string(message.Value) == "fail" { // Simulate a failure
				return fmt.Errorf("error")
			}

			session.MarkMessage(message, "")
		}
	}
}
