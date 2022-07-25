package kafka

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
)

//const kafkaAddress = "localhost:9092"
type Offset int64

const (
	Beginning Offset = -2
	End       Offset = -1
)

type Message struct {
	Images   [][]byte
	Audio    []byte
	Commands [][]byte
}

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

func NewKafkaConsumerOnMultipleTopics(topics []string, groupId string, offset Offset) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{kafkaAddress},
				MinBytes:    1,
				MaxBytes:    10e6,
				GroupID:     groupId,
				GroupTopics: topics,
				StartOffset: int64(offset),
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
