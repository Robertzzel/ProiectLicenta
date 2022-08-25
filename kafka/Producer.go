package kafka

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
	"time"
)

const (
	kafkaAddress = "localhost:9092"
	TimeFormat   = "2006-01-02T15:04:05.999999999Z07:00" // RFC3339Nano
)

type Producer struct {
	*kafka.Writer
}

func NewKafkaProducer(topic string) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaAddress),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func NewVideoKafkaProducer(topic string) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaAddress),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: 1,
			Async:        true,
		},
	}
}

func NewSyncKafkaProducer(topic string) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaAddress),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: 1,
			Async:        false,
		},
	}
}

func (kp *Producer) Publish(message []byte) error {
	return kp.WriteMessages(
		context.Background(),
		kafka.Message{
			Value: message,
		},
	)
}

func (kp *Producer) PublishWithTimestamp(message []byte) error {
	return kp.WriteMessages(
		context.Background(),
		kafka.Message{
			Value: message,
			Headers: []kafka.Header{{
				Key:   "timestamp",
				Value: []byte(time.Now().Format(TimeFormat)),
			}},
		},
	)
}

func NewInterAppProducer(topic string) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaAddress),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: 1,
			Async:        true,
		},
	}
}
