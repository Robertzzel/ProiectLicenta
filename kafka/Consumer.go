package kafka

import (
	"context"
	"encoding/json"
	kafka "github.com/segmentio/kafka-go"
	"time"
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

func (kc *Consumer) SetOffsetToNow() error {
	return kc.Reader.SetOffsetAt(context.Background(), time.Now())
}

type InterAppConsumer struct {
	*Consumer
}

func NewInterAppConsumer(topic string) *InterAppConsumer {
	return &InterAppConsumer{
		Consumer: NewKafkaConsumer(topic),
	}
}

func (c *InterAppConsumer) Consume() (InterAppMessage, error) {
	encodedMessage, err := c.Consumer.Consume()
	if err != nil {
		return InterAppMessage{}, err
	}

	decodedMessage := &InterAppMessage{}
	err = json.Unmarshal(encodedMessage.Value, decodedMessage)
	if err != nil {
		return InterAppMessage{}, err
	}

	return *decodedMessage, nil
}
