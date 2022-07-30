package kafka

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
)

type Consumer struct {
	*kafka.Reader
	Topic string
}

func NewKafkaConsumer(topic string) *Consumer {
	return &Consumer{
		Reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  []string{kafkaAddress},
				Topic:    topic,
				MinBytes: 1,
				MaxBytes: 10e6 * 3,
			},
		),
		Topic: topic,
	}
}

func (kc *Consumer) Consume() (Message, error) {
	message, err := kc.ReadMessage(context.Background())
	if err != nil {
		return Message{}, err
	}

	return Message{message}, nil
}
