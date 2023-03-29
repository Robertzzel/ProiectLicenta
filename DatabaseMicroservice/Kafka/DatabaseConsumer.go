package Kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
)

type DatabaseConsumer struct {
	*ConsumerWrapper
}

func (this *DatabaseConsumer) Consume(ctx context.Context) (*kafka.Message, error) {
	for ctx.Err() == nil {
		ev := this.Poll(200)
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

func (this *DatabaseConsumer) ConsumeFullMessage(ctx context.Context) ([]byte, []kafka.Header, error) {
	message, err := this.Consume(ctx)
	if err != nil {
		return nil, nil, err
	}

	var numberOfMessages, messageNumber int
	for _, header := range message.Headers {
		if header.Key == "number-of-messages" {
			numberOfMessages, err = strconv.Atoi(string(header.Value))
			if err != nil {
				return nil, nil, err
			}
		} else if header.Key == "message-number" {
			messageNumber, err = strconv.Atoi(string(header.Value))
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if numberOfMessages == 0 {
		return message.Value, message.Headers, nil
	}

	messages := make([][]byte, numberOfMessages)
	messages[messageNumber] = message.Value

	for i := 0; i < numberOfMessages-1; i++ {
		message, err = this.Consume(ctx)
		if err != nil {
			return nil, nil, err
		}

		for _, header := range message.Headers {
			if header.Key == "message-number" {
				messageNumber, err = strconv.Atoi(string(header.Value))
				if err != nil {
					return nil, nil, err
				}
			}
		}

		messages[messageNumber] = message.Value
	}

	fullMessage := make([]byte, 0)
	for _, messageValues := range messages {
		fullMessage = append(fullMessage, messageValues...)
	}

	return fullMessage, message.Headers, nil
}

func NewDatabaseConsumer(brokerAddress, topic string) (*DatabaseConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"group.id":          "-",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		return nil, err
	}

	if err = consumer.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return &DatabaseConsumer{&ConsumerWrapper{consumer}}, nil
}
