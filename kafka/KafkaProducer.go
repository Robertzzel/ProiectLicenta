package kafka

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
)

const kafkaAddress = "localhost:9092"

type KafkaProducer struct {
	*kafka.Writer
}

func NewKafkaProducer(topic string) *KafkaProducer {
	return &KafkaProducer{
		Writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaAddress),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (kp *KafkaProducer) Publish(message []byte) error {
	return kp.WriteMessages(
		context.Background(),
		kafka.Message{
			Value: message,
		},
	)
}
