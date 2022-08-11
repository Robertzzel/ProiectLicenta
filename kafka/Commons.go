package kafka

import (
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
	"time"
)

type InterAppMessage struct {
	Images    [][]byte
	Audio     []byte
	Inputs    [][]byte
	Timestamp time.Time
}

type Message struct {
	kafka.Message
}

func CreateTopic(topic string) error {
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
			NumPartitions:     1,
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
