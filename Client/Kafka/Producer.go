package Kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math"
)

type Producer struct {
	*kafka.Producer
	deliveryChannel chan kafka.Event
}

var MaxMessageBytes = 5 * 1024 * 1024

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (producer *Producer) Publish(message []byte, headers []kafka.Header, topic string, partition int32) error {
	numberOfMessages := int(math.Ceil(float64(len(message)) / float64(MaxMessageBytes)))
	headersT := append(headers, kafka.Header{Key: "number-of-messages", Value: []byte(fmt.Sprintf("%05d", numberOfMessages))})
	tp := kafka.TopicPartition{Topic: &topic, Partition: partition}

	for i := 0; i < numberOfMessages; i++ {
		err := producer.Producer.Produce(
			&kafka.Message{
				TopicPartition: tp,
				Value:          message[i*MaxMessageBytes : min(len(message), (i+1)*MaxMessageBytes)],
				Headers:        append(headersT, []kafka.Header{{Key: "message-number", Value: []byte(fmt.Sprintf("%05d", i))}}...)},
			producer.deliveryChannel,
		)
		if err != nil {
			return err
		}
		go func() { <-producer.deliveryChannel }()
	}

	return nil
}

func NewProducer(brokerAddress string) (Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":       brokerAddress,
		"group.id":                "-",
		"acks":                    "all",
		"fetch.message.max.bytes": MaxMessageBytes,
	})
	if err != nil {
		return Producer{}, err
	}
	return Producer{p, make(chan kafka.Event, 3)}, nil
}
