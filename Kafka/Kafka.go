package Kafka

import (
	"errors"
	kafka "github.com/Shopify/sarama"
)

const (
	brokerAddress = "localhost:9092"
)

type Producer struct {
	kafkaProducer kafka.AsyncProducer
}

type Consumer struct {
	kafkaConsumer kafka.PartitionConsumer
}

func NewProducer() (*Producer, error) {
	config := kafka.NewConfig()
	config.Producer.RequiredAcks = kafka.WaitForLocal
	config.Producer.Partitioner = kafka.NewRoundRobinPartitioner

	p, err := kafka.NewAsyncProducer([]string{brokerAddress}, config)
	if err != nil {
		return nil, err
	}

	return &Producer{p}, nil
}

func (producer *Producer) Publish(message []byte, topic string) {
	producer.kafkaProducer.Input() <- &kafka.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     kafka.ByteEncoder(message),
	}
}

func (producer *Producer) Close() error {
	return producer.kafkaProducer.Close()
}

func NewConsumer(topic string) (*Consumer, error) {
	c, err := kafka.NewConsumer([]string{brokerAddress}, nil)
	if err != nil {
		return nil, err
	}
	partitions, err := c.Partitions(topic)

	pc, err := c.ConsumePartition(topic, partitions[0], kafka.OffsetOldest)
	if err != nil {
		return nil, err
	}

	return &Consumer{pc}, nil
}

func (consumer *Consumer) Consume() ([]byte, error) {
	message := <-consumer.kafkaConsumer.Messages()
	if message == nil {
		return nil, errors.New("message is nil")
	}
	return message.Value, nil
}

func (consumer *Consumer) Close() error {
	return consumer.kafkaConsumer.Close()
}

func DeleteTopic(topic string) error {
	clusterAdmin, err := kafka.NewClusterAdmin([]string{brokerAddress}, nil)
	if err != nil {
		return err
	}

	return clusterAdmin.DeleteTopic(topic)
}
