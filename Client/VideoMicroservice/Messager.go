package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"licenta/Kafka"
)

type KafkaConnection struct {
	producer Kafka.Producer
	consumer Kafka.Consumer
	topic    string
}

func (conn *KafkaConnection) Publish(message []byte, headers []kafka.Header) error {
	return conn.producer.Publish(message, headers, conn.topic, Kafka.AggregatorMicroservicePartition)
}

func (conn *KafkaConnection) Consume(ctx context.Context) (*kafka.Message, error) {
	return conn.consumer.Consume(ctx)
}

func NewKafkaConnection(brokerAddress, topic string) (KafkaConnection, error) {
	producer, err := Kafka.NewProducer(brokerAddress)
	if err != nil {
		return KafkaConnection{}, err
	}

	consumer, err := Kafka.NewConsumer(brokerAddress)
	if err != nil {
		return KafkaConnection{}, err
	}

	if err = consumer.Assign([]kafka.TopicPartition{
		{Topic: &topic, Partition: Kafka.VideoMicroservicePartition},
	}); err != nil {
		panic(err)
	}

	return KafkaConnection{
		producer,
		consumer,
		topic}, nil
}

func (conn *KafkaConnection) Flush(timeoutMs int) int {
	return conn.producer.Flush(timeoutMs)
}

func (conn *KafkaConnection) Close() error {
	conn.producer.Close()
	return conn.consumer.Close()
}
