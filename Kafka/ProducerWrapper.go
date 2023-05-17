package Kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math"
)

type ProducerWrapper struct {
	*kafka.Producer
}

func (producer *ProducerWrapper) Publish(message []byte, headers []kafka.Header, topic string, partition int32, deliveryChannel chan kafka.Event) error {
	numberOfMessages := int(math.Ceil(float64(len(message)) / float64(MaxMessageBytes)))

	for i := 0; i < numberOfMessages; i++ {
		err := producer.Producer.Produce(
			&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
				Value:          message[i*MaxMessageBytes : min(len(message), (i+1)*MaxMessageBytes)],
				Headers: append(headers, []kafka.Header{
					{Key: "number-of-messages", Value: []byte(fmt.Sprintf("%05d", numberOfMessages))},
					{Key: "message-number", Value: []byte(fmt.Sprintf("%05d", i))}}...),
			},
			deliveryChannel,
		)
		if err != nil {
			return err
		}
		go func() { <-deliveryChannel }()
	}

	return nil
}
