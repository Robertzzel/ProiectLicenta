package Kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type DatabaseProducer struct {
	*ProducerWrapper
	deliverChannel chan kafka.Event
}

func (producer *DatabaseProducer) Publish(message []byte, headers []kafka.Header, topic string, partition int32) error {
	return producer.ProducerWrapper.Publish(message, headers, topic, partition, producer.deliverChannel)
}

func NewDatabaseProducer(brokerAddress string) (*DatabaseProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"client.id":         "-",
		"acks":              "all",
	})
	if err != nil {
		return nil, err
	}

	return &DatabaseProducer{ProducerWrapper: &ProducerWrapper{p}, deliverChannel: make(chan kafka.Event, 5)}, nil
}
