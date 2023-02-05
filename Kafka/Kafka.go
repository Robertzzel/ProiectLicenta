package Kafka

import (
	"context"
	"fmt"
	kafkago "github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

const (
	brokerNetwork   = "tcp"
	MaxMessageBytes = 600_000
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

type Header = kafkago.Header

type ConsumerMessage struct {
	Message []byte
	Headers []Header
}

type ProducerMessage struct {
	Message []byte
	Topic   string
	Headers []Header
}

type Producer struct {
	kafkaWriter *kafkago.Writer
}

type Consumer struct {
	kafkaReader *kafkago.Reader
}

func NewProducer(brokerAddress string) *Producer {
	return &Producer{
		kafkaWriter: &kafkago.Writer{
			Addr:         kafkago.TCP(brokerAddress),
			Async:        true,
			Balancer:     &kafkago.LeastBytes{},
			BatchSize:    1,
			RequiredAcks: kafkago.RequireAll,
		},
	}
}

func (producer *Producer) Publish(producerMessage *ProducerMessage) error {
	numberOfMessages := 0

	if len(producerMessage.Message) < MaxMessageBytes {
		numberOfMessages = 1
	} else if len(producerMessage.Message)%MaxMessageBytes == 0 {
		numberOfMessages = len(producerMessage.Message) / MaxMessageBytes
	} else {
		numberOfMessages = len(producerMessage.Message)/MaxMessageBytes + 1
	}

	messages := make([]kafkago.Message, numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		messages[i] = kafkago.Message{
			Value: producerMessage.Message[i*MaxMessageBytes : min(len(producerMessage.Message), (i+1)*MaxMessageBytes)],
			Topic: producerMessage.Topic,
			Headers: append([]kafkago.Header{
				{Key: "number-of-messages", Value: []byte(fmt.Sprintf("%05d", numberOfMessages))},
				{Key: "message-number", Value: []byte(fmt.Sprintf("%05d", i))},
			}, producerMessage.Headers...),
		}
	}

	return producer.kafkaWriter.WriteMessages(context.Background(), messages...)
}

func (producer *Producer) Close() error {
	return producer.kafkaWriter.Close()
}

func NewConsumer(brokerAddress, topic string) *Consumer {
	return &Consumer{
		kafkaReader: kafkago.NewReader(
			kafkago.ReaderConfig{
				Brokers:     []string{brokerAddress},
				Topic:       topic,
				StartOffset: kafkago.LastOffset,
			},
		),
	}
}

func (kc *Consumer) Consume(timeout ...time.Duration) (*ConsumerMessage, error) {
	var ctx context.Context

	if len(timeout) > 0 {
		ctx, _ = context.WithTimeout(context.Background(), timeout[0])
	} else {
		ctx = context.Background()
	}

	message, err := kc.kafkaReader.ReadMessage(ctx)
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

	if numberOfMessages == 0 {
		return &ConsumerMessage{Message: message.Value, Headers: message.Headers}, nil
	}

	messages := make([][]byte, numberOfMessages)
	messages[messageNumber] = message.Value

	for i := 0; i < numberOfMessages-1; i++ {
		message, err = kc.kafkaReader.ReadMessage(context.Background())
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

func (kc *Consumer) Close() error {
	return kc.kafkaReader.Close()
}

func (kc *Consumer) SetOffsetToNow() error {
	return kc.kafkaReader.SetOffsetAt(context.Background(), time.Now().Add(-time.Second))
}

func CreateTopic(brokerAddress, name string) error {
	_, err := kafkago.DialLeader(context.Background(), brokerNetwork, brokerAddress, name, 0)
	return err
}

func DeleteTopic(brokerAddress string, names ...string) error {
	conn, err := kafkago.DialLeader(context.Background(), brokerNetwork, brokerAddress, names[0], 0)
	if err != nil {
		return err
	}

	return conn.DeleteTopics(names...)
}
