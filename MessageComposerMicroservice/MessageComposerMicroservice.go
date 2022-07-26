package main

import (
	"Licenta/kafka"
	"fmt"
	"time"
)

const (
	kafkaImagesTopic   = "images"
	kafkaAudioTopic    = "audio"
	kafkaInputTopic    = "input"
	kafkaMessagesTopic = "messages"
)

func createKafkaTopics() error {
	err := kafka.CreateTopic(kafkaImagesTopic, 1)
	if err != nil {
		return err
	}
	err = kafka.CreateTopic(kafkaAudioTopic, 2)
	if err != nil {
		return err
	}
	err = kafka.CreateTopic(kafkaInputTopic, 1)
	if err != nil {
		return err
	}
	err = kafka.CreateTopic(kafkaMessagesTopic, 1)
	if err != nil {
		return err
	}

	return nil
}

func syncMessages(consumer *kafka.KafkaConsumer) error {
	startTime := time.Now()
	fmt.Println("Syncing on", startTime)
	for {
		message, err := consumer.Consume()
		if err != nil {
			return err
		}

		if message.Topic == "audio" && message.Time.After(startTime) {
			break
		}
	}
	fmt.Println("Sync done")
	return nil
}

func main() {
	err := createKafkaTopics()
	if err != nil {
		fmt.Println(err)
	}

	kafkaConsumer := kafka.NewKafkaConsumerOnMultipleTopics(
		[]string{kafkaImagesTopic, kafkaInputTopic, kafkaAudioTopic},
		"composer",
		kafka.End,
	)
	kafkaProducer := kafka.NewKafkaProducer(kafkaMessagesTopic)

	err = syncMessages(kafkaConsumer)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		sendingMessage := kafka.Message{}

	messageBuilderLoop:
		for {
			message, err := kafkaConsumer.Consume()
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(message.Topic)

			switch message.Topic {
			case kafkaImagesTopic:
				sendingMessage.Images = append(sendingMessage.Images, message.Value)
			case kafkaAudioTopic:
				sendingMessage.Audio = message.Value
				break messageBuilderLoop
			case kafkaInputTopic:
				sendingMessage.Commands = append(sendingMessage.Commands, message.Value)
			}
		}

		fmt.Println(len(sendingMessage.Images), len(sendingMessage.Audio))

		err = kafkaProducer.PublishMessage(sendingMessage)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
