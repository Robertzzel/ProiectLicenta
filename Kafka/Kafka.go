package Kafka

import (
	"context"
	kafkago "github.com/segmentio/kafka-go"
	"time"
)

const (
	brokerAddress = "localhost:9092"
	brokerNetwork = "tcp"
)

type Producer struct {
	*kafkago.Writer
}

type Consumer struct {
	*kafkago.Reader
}

func NewProducer(topic string) *Producer {
	return &Producer{
		Writer: &kafkago.Writer{
			Addr:      kafkago.TCP(brokerAddress),
			Topic:     topic,
			Balancer:  &kafkago.LeastBytes{},
			BatchSize: 1,
		},
	}
}

func NewProducerAsync(topic string) *Producer {
	return &Producer{
		Writer: &kafkago.Writer{
			Addr:      kafkago.TCP(brokerAddress),
			Topic:     topic,
			Async:     true,
			Balancer:  &kafkago.LeastBytes{},
			BatchSize: 1,
		},
	}
}

func (kp *Producer) Publish(message []byte) error {
	return kp.WriteMessages(context.Background(), kafkago.Message{Value: message})
}

func NewConsumer(topic string) *Consumer {
	return &Consumer{
		Reader: kafkago.NewReader(
			kafkago.ReaderConfig{
				Brokers:     []string{brokerAddress},
				Topic:       topic,
				StartOffset: kafkago.LastOffset,
			},
		),
	}
}

func (kc *Consumer) Consume() (kafkago.Message, error) {
	message, err := kc.ReadMessage(context.Background())
	if err != nil {
		return kafkago.Message{}, err
	}

	return message, nil
}

func (kc *Consumer) SetOffsetToNow() error {
	return kc.Reader.SetOffsetAt(context.Background(), time.Now())
}

func CreateTopic(name string) error {
	_, err := kafkago.DialLeader(context.Background(), brokerNetwork, brokerAddress, name, 0)
	return err
}

func DeleteTopic(names ...string) error {
	conn, err := kafkago.DialLeader(context.Background(), brokerNetwork, brokerAddress, names[0], 0)
	if err != nil {
		return err
	}

	return conn.DeleteTopics(names...)
}
