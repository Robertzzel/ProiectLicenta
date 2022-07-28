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

type InterAppMessage struct {
	Images [][]byte
	Audio  []byte
	Inputs [][]byte
}

type KafkaProducer struct {
	*kafka.Writer
	*json.Encoder
	*bytes.Buffer
}

func NewKafkaProducer(topic string) *KafkaProducer {
	var buffer bytes.Buffer

	return &KafkaProducer{
		Writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaAddress),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		Encoder: json.NewEncoder(&buffer),
		Buffer:  &buffer,
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

func (kp *KafkaProducer) PublishWithTimestamp(message []byte) error {
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

func (kp *KafkaProducer) PublishMessage(message Message) error {
	kp.Buffer.Truncate(0)
	err := kp.Encoder.Encode(message)
	if err != nil {
		return err
	}

	return kp.Publish(kp.Buffer.Bytes())
}
