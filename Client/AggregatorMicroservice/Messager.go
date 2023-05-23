package main

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"licenta/Kafka"
)

type KafkaConnection struct {
	producer Kafka.Producer
	consumer Kafka.Consumer
	topic    string
}

func (conn *KafkaConnection) PublishClient(message []byte, headers []kafka.Header) error {
	return conn.producer.Publish(message, headers, conn.topic, Kafka.ClientPartition)
}

func (conn *KafkaConnection) PublishVideoStart(message []byte, headers []kafka.Header) error {
	return conn.producer.Publish(message, headers, conn.topic, Kafka.VideoMicroservicePartition)
}

func (conn *KafkaConnection) PublishAudioStart(message []byte, headers []kafka.Header) error {
	return conn.producer.Publish(message, headers, conn.topic, Kafka.AudioMicroservicePartition)
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
		{Topic: &topic, Partition: Kafka.AggregatorMicroservicePartition},
	}); err != nil {
		panic(err)
	}

	return KafkaConnection{
		producer,
		consumer,
		topic}, nil
}

func (conn *KafkaConnection) GetTypeFromMessage(msg *kafka.Message) (string, error) {
	for _, header := range msg.Headers {
		if header.Key == "type" {
			return string(header.Value), nil
		}
	}
	return "", errors.New("no type received")
}

func (conn *KafkaConnection) Flush(timeoutMs int) int {
	return conn.producer.Flush(timeoutMs)
}

func (conn *KafkaConnection) Close() error {
	conn.producer.Close()
	return conn.consumer.Close()
}
