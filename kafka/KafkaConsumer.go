package kafka

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
)

//const kafkaAddress = "localhost:9092"

type KafkaConsumer struct {
	*kafka.Reader
}

func NewKafkaConsumer(topic string) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  []string{kafkaAddress},
				Topic:    topic,
				MinBytes: 1,
				MaxBytes: 10e6,
			},
		),
	}
}

func (kc *KafkaConsumer) Consume() (kafka.Message, error) {
	message, err := kc.ReadMessage(context.Background())
	if err != nil {
		return kafka.Message{}, err
	}

	return message, nil
}
