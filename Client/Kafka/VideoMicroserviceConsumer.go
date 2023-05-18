package Kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type VideoMicroserviceConsumer struct {
	*ConsumerWrapper
}

func (consumer *VideoMicroserviceConsumer) Consume(timeout ...time.Duration) ([]byte, []kafka.Header, error) {
	return consumer.Consume(timeout...)
}

func NewVideoMicroserviceConsumer(brokerAddress, topic string) (*VideoMicroserviceConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"group.id":          "-",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		return nil, err
	}

	if err = consumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: VideoMicroservicePartition}}); err != nil {
		panic(err)
	}

	return &VideoMicroserviceConsumer{&ConsumerWrapper{consumer}}, nil
}
