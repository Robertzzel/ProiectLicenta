package Kafka

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
	"time"
)

const (
	VideoMessage = 0
	AudioMessage = 1
)

type AggregatorMicroserviceConsumer struct {
	*ConsumerWrapper
}

func (this *AggregatorMicroserviceConsumer) Consume(ctx context.Context, partition int32) (*kafka.Message, error) {
	for ctx.Err() == nil { // context activ
		ev := this.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			if e.TopicPartition.Partition == partition {
				return e, nil
			}
		case kafka.Error:
			return nil, e
		default:
		}
	}
	return nil, ctx.Err()
}

func (this *AggregatorMicroserviceConsumer) ConsumeFullMessage(ctx context.Context, partition int32) ([]byte, []kafka.Header, error) {
	message, err := this.Consume(ctx, partition)
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
		message, err = this.Consume(ctx, partition)
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

func (this *AggregatorMicroserviceConsumer) ConsumeAggregatorStart(timeout ...time.Duration) ([]byte, []kafka.Header, error) { // to start the aggregator
	var ctx context.Context

	if len(timeout) > 0 {
		ctx, _ = context.WithTimeout(context.Background(), timeout[0])
	} else {
		ctx = context.Background()
	}

	return this.ConsumeFullMessage(ctx, AggregatorMicroserviceStartPartition)
}

func (this *AggregatorMicroserviceConsumer) ConsumeAggregator(timeout ...time.Duration) ([]byte, int, error) { // to receive video/audio
	var ctx context.Context

	if len(timeout) > 0 {
		ctx, _ = context.WithTimeout(context.Background(), timeout[0])
	} else {
		ctx = context.Background()
	}

	msg, headers, err := this.ConsumeFullMessage(ctx, AggregatorMicroservicePartition)
	if ctx.Err() != nil {
		return nil, 0, ctx.Err()
	}

	var msgType = ""
	for _, header := range headers {
		if header.Key == "type" {
			msgType = string(header.Value)
		}
	}

	println("#", msgType, "# ", len(msg))
	if msgType == "video" {
		return msg, VideoMessage, err
	} else if msgType == "audio" {
		return msg, AudioMessage, err
	} else {
		return nil, 0, errors.New("no header")
	}
}

func NewAggregatorMicroserviceConsumer(brokerAddress, topic string) (*AggregatorMicroserviceConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"group.id":          "-",
		"auto.offset.reset": "latest",
	})
	if err != nil {
		return nil, err
	}

	if err = consumer.Assign([]kafka.TopicPartition{
		{Topic: &topic, Partition: AggregatorMicroservicePartition},
		{Topic: &topic, Partition: AggregatorMicroserviceStartPartition},
	}); err != nil {
		panic(err)
	}

	return &AggregatorMicroserviceConsumer{&ConsumerWrapper{consumer}}, nil
}
