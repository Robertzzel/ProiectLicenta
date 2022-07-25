package main

import (
	"Licenta/kafka"
	"fmt"
)

const (
	kafkaImagesTopic   = "images"
	kafkaAudioTopic    = "audio"
	kafkaInputTopic    = "input"
	kafkaMessagesTopic = "messages"
)

func main() {
	fmt.Println("starting")

	kafkaAudioConsumer := kafka.NewKafkaConsumerOnMultipleTopics([]string{kafkaAudioTopic}, "composer8", kafka.End)
	kafkaConsumer := kafka.NewKafkaConsumerOnMultipleTopics(
		[]string{kafkaImagesTopic, kafkaInputTopic, kafkaAudioTopic},
		"composer9",
		kafka.End,
	)
	kafkaProducer := kafka.NewKafkaProducer(kafkaMessagesTopic)

	fmt.Println("Wait for sync")
	_, err := kafkaAudioConsumer.Consume()
	if err != nil {
		fmt.Println("Nu se poate face sincronizarea", err)
		return
	}
	fmt.Println("Sync done")

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
			fmt.Println(err) /// EROARE PE AICI VEZI DATA VIITOATE
			return
		}
	}
}
