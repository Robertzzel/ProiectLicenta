package Kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math"
	"strconv"
	"time"
)

const (
	brokerAddress   = "localhost:9092"
	brokerNetwork   = "tcp"
	MaxMessageBytes = 1000000
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

type Header = kafka.Header

type ConsumerMessage struct {
	Message []byte
	Headers []Header
}

type ProducerMessage struct {
	Message   []byte
	Topic     string
	Headers   []Header
	Partition uint
}

type Producer struct {
	*kafka.Producer
}

type Consumer struct {
	*kafka.Consumer
}

func NewProducer() (*Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokerAddress,
		"acks":               "0",
		"batch.size":         "1",
		"linger.ms":          "0",
		"batch.num.messages": "1",
	})
	if err != nil {
		return nil, err
	}
	return &Producer{producer}, nil
}

func (producer *Producer) Publish(producerMessage *ProducerMessage) error {
	numberOfMessages := int(math.Ceil(float64(len(producerMessage.Message)) / float64(MaxMessageBytes)))

	messages := make([]*kafka.Message, numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		messages[i] = &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &producerMessage.Topic, Partition: int32(producerMessage.Partition)},
			Value:          producerMessage.Message[i*MaxMessageBytes : min(len(producerMessage.Message), (i+1)*MaxMessageBytes)],
			Headers: append([]kafka.Header{
				{Key: "number-of-messages", Value: []byte(fmt.Sprintf("%05d", numberOfMessages))},
				{Key: "message-number", Value: []byte(fmt.Sprintf("%05d", i))},
			}, producerMessage.Headers...),
		}
	}

	deliveryChan := make(chan kafka.Event, numberOfMessages+1)
	for _, message := range messages {
		err := producer.Produce(message, deliveryChan)
		if err != nil {
			return err
		}
	}

	return nil
}

func (producer *Producer) Close() error {
	producer.Flush(1000)
	producer.Producer.Close()
	return nil
}

func NewConsumer(topic string, partition uint) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"auto.offset.reset": "latest",
		"group.id":          "no",
	})
	if err != nil {
		return nil, err
	}
	if err := consumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: int32(partition)}}); err != nil {
		return nil, err
	}

	return &Consumer{consumer}, err
}

func (consumer *Consumer) Consume(timeout ...time.Duration) (*ConsumerMessage, error) {
	var to = 0
	if len(timeout) > 0 {
		to = int(timeout[0] / time.Millisecond)
	}
	message, err := consumer.getNextMessage(to)
	if err != nil {
		return nil, err
	}

	var numberOfMessages, messageNumber int
	for _, header := range message.Headers {
		if header.Key == "number-of-messages" {
			numberOfMessages, err = strconv.Atoi(string(header.Value))
			if err != nil {
				return nil, err
			}
		} else if header.Key == "message-number" {
			messageNumber, err = strconv.Atoi(string(header.Value))
			if err != nil {
				return nil, err
			}
		}
	}

	if numberOfMessages == 1 || numberOfMessages == 0 {
		return &ConsumerMessage{Message: message.Value, Headers: message.Headers}, nil
	}

	messages := make([][]byte, numberOfMessages)
	messages[messageNumber] = message.Value

	for i := 0; i < numberOfMessages-1; i++ {
		message, err = consumer.getNextMessage(to)
		if err != nil {
			return nil, err
		}

		for _, header := range message.Headers {
			if header.Key == "message-number" {
				messageNumber, err = strconv.Atoi(string(header.Value))
				if err != nil {
					return nil, err
				}
			}
		}

		messages[messageNumber] = message.Value
	}

	fullMessage := make([]byte, 0)
	for _, messageValues := range messages {
		fullMessage = append(fullMessage, messageValues...)
	}

	return &ConsumerMessage{Message: fullMessage, Headers: message.Headers}, nil
}

func (consumer *Consumer) getNextMessage(timeoutMs int) (*kafka.Message, error) {

	for {
		ev := consumer.Poll(timeoutMs)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Println(string(e.Value))
			return e, nil
		case kafka.Error:
			return nil, e
		default:
			return nil, context.DeadlineExceeded
		}
	}
}

func (consumer *Consumer) Close() error {
	return consumer.Consumer.Close()
}

func CreateTopic(name string, numOfPartitions uint) error {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokerAddress})
	if err != nil {
		return err
	}

	_, err = a.CreateTopics(context.Background(), []kafka.TopicSpecification{{Topic: name, NumPartitions: int(numOfPartitions), Config: map[string]string{"retention.ms": "100"}}})
	if err != nil {
		return err
	}

	return nil
}

func DeleteTopic(names ...string) error {

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokerAddress})
	if err != nil {
		return err
	}

	_, err = a.DeleteTopics(context.Background(), names)
	if err != nil {
		return err
	}

	return nil
}
