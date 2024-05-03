package saramahealth

import "github.com/IBM/sarama"

type Config struct {
	Brokers      []string
	Topics       []string
	SaramaConfig *sarama.Config
}
