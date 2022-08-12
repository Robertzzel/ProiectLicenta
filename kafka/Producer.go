package kafka

import (
	"bytes"
	"context"
	"encoding/json"
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

func NewImageKafkaProducer(topic string) *Producer {
	return &Producer{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaAddress),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: 0,
			Async:        true,
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

type InterAppProducer struct {
	*kafka.Writer
	*json.Encoder
	*bytes.Buffer
}

func NewInterAppProducer(topic string) *InterAppProducer {
	var buffer bytes.Buffer

	return &InterAppProducer{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaAddress),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: 0,
			Async:        true,
		},
		Encoder: json.NewEncoder(&buffer),
		Buffer:  &buffer,
	}
}

func (kp *InterAppProducer) Publish(message InterAppMessage) error {
	kp.Buffer.Truncate(0)
	err := kp.Encoder.Encode(message)
	if err != nil {
		return err
	}

	encodedMessage := string(kp.Buffer.Bytes())
	return kp.WriteMessages(
		context.Background(),
		kafka.Message{
			Value: []byte(encodedMessage),
		},
	)
}
