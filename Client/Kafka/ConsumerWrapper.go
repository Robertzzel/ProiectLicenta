package Kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
	"time"
)

type ConsumerWrapper struct {
	*kafka.Consumer
}

func (consumer *ConsumerWrapper) Consume(ctx context.Context) (*kafka.Message, error) {
	for ctx.Err() == nil { // context activ
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

func (consumer *ConsumerWrapper) ConsumeFullMessage(timeout ...time.Duration) ([]byte, []kafka.Header, error) {
	var ctx context.Context

	if len(timeout) > 0 {
		ctx, _ = context.WithTimeout(context.Background(), timeout[0])
	} else {
		ctx = context.Background()
	}

	message, err := consumer.Consume(ctx)
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
		message, err = consumer.Consume(context.Background())
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
