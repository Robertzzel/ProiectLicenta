package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	kafka "github.com/segmentio/kafka-go"
)

const kafkaAddress = "localhost:9092"

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

func (kp *KafkaProducer) PublishMessage(message Message) error {
	kp.Buffer.Truncate(0)
	err := kp.Encoder.Encode(message)
	if err != nil {
		return err
	}

	return kp.Publish(kp.Buffer.Bytes())
}
