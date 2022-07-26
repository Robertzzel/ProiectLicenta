package kafka

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type Offset int64

const (
	Beginning Offset = -2
	End       Offset = -1
)

type Message struct {
	Images   [][]byte
	Audio    []byte
	Commands [][]byte
}

type KafkaConsumer struct {
	*kafka.Reader
}

func NewKafkaConsumer(topic string) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  []string{kafkaAddress},
				Topic:    topic,
				MinBytes: 1,
				MaxBytes: 10e6,
			},
		),
	}
}

func NewKafkaConsumerOnMultipleTopics(topics []string, groupId string, offset Offset) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{kafkaAddress},
				MinBytes:    1,
				MaxBytes:    10e6,
				GroupID:     groupId,
				GroupTopics: topics,
				StartOffset: int64(offset),
			},
		),
	}
}

func (kc *KafkaConsumer) Consume() (kafka.Message, error) {
	message, err := kc.ReadMessage(context.Background())
	if err != nil {
		return kafka.Message{}, err
	}

	return message, nil
}

func (kc *KafkaConsumer) SetOffsetNow() error {
	conn, err := kafka.Dial("tcp", kafkaAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		return err
	}
	fmt.Println(offset)

	return nil
}

func CreateTopic(topic string, numberOfPartitions int) error {
	conn, err := kafka.Dial("tcp", kafkaAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     numberOfPartitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	return nil
}

func DeleteTopics(topics []string) error {
	conn, err := kafka.Dial("tcp", kafkaAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.DeleteTopics(topics...)
	if err != nil {
		return err
	}

	return nil
}
