package Kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	*kafka.Consumer
}

func (consumer *Consumer) Consume(ctx context.Context) (*kafka.Message, error) {
	for ctx.Err() == nil {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			return e, nil
		case kafka.Error:
			return nil, e
		default:
		}
	}
	return nil, ctx.Err()
}

func NewConsumer(brokerAddress, certificatePath string) (Consumer, error) {
	p, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                     brokerAddress,
		"group.id":                              "-",
		"acks":                                  "all",
		"fetch.message.max.bytes":               MaxMessageBytes,
		"security.protocol":                     "SSL",
		"ssl.ca.location":                       certificatePath, //"/home/robert/Workspace/kafka_2.13-3.2.0/keys/Client/truststore.pem",
		"ssl.endpoint.identification.algorithm": "none",
	})
	if err != nil {
		return Consumer{}, err
	}
	return Consumer{p}, nil
}

func (consumer *Consumer) AssignTopicPartition(topic string, partition int32) error {
	return consumer.Assign([]kafka.TopicPartition{
		{Topic: &topic, Partition: partition},
	})
}
