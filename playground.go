package main

import (
	kafka "github.com/Shopify/sarama"
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
	config.Producer.Partitioner = kafka.NewRandomPartitioner

	p, err := kafka.NewAsyncProducer([]string{"localhost:9092"}, config)
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
	c, err := kafka.NewConsumer([]string{"localhost:9092"}, nil)
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

func (consumer *Consumer) Consume() []byte {
	return (<-consumer.kafkaConsumer.Messages()).Value
}

func (consumer *Consumer) Close() error {
	return consumer.kafkaConsumer.Close()
}

func DeleteTopic(topic string) {
	clusterAdmin, _ := kafka.NewClusterAdmin([]string{"localhost:9092"}, nil)
	if err := clusterAdmin.DeleteTopic(topic); err != nil {
		panic(err)
	}
}

func main() {
	topic := "sal"

	// prod
	p, err := NewProducer()
	if err != nil {
		panic(err)
	}
	defer p.Close()

	p.Publish([]byte("mesajdeKafka"), topic)
	//consumer

	consumer, err := NewConsumer(topic)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// delete topic

}
