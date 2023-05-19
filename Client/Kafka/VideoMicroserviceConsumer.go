package Kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type VideoMicroserviceConsumer struct {
	ConsumerWrapper
}

func (consumer *VideoMicroserviceConsumer) Consume(ctx context.Context) ([]byte, []kafka.Header, error) {
	return consumer.ConsumerWrapper.Consume(ctx)
}

func NewVideoMicroserviceConsumer(brokerAddress, topic string) (*VideoMicroserviceConsumer, error) {
	consumer, err := NewConsumer(brokerAddress)
	if err != nil {
		return nil, err
	}

	if err = consumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: VideoMicroservicePartition}}); err != nil {
		panic(err)
	}

	return &VideoMicroserviceConsumer{consumer}, nil
}
