package Kafka

import (
	"context"
	. "github.com/segmentio/kafka-go"
)

func CreateTopic(brokerAddress, name string) error {
	_, err := DialLeader(context.Background(), "tcp", brokerAddress, name, 0)
	return err
}
