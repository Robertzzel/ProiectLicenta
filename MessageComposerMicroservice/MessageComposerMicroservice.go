package main

import (
	"Licenta/kafka"
	"context"
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
	err = kafka.CreateTopic(kafkaAudioTopic, 1)
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

func main() {
	videoBuffer := make([]kafka.Message, 0)
	audioBuffer := make([]byte, 0)
	inputBuffer := make([]kafka.Message, 0)
	var audioBufferStartTime time.Time

	err := createKafkaTopics()
	if err != nil {
		fmt.Println(err)
	}

	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	videoConsumer := kafka.NewKafkaConsumer(kafkaImagesTopic)
	inputConsumer := kafka.NewKafkaConsumer(kafkaInputTopic)

	err = audioConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	err = videoConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	err = inputConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}

	go func() {
		for {
			message, err := videoConsumer.Consume()
			if err != nil {
				fmt.Println("Error while consuming video: ", err)
				return
			}
			videoBuffer = append(videoBuffer, message)
		}
	}()

	go func() {
		message, err := audioConsumer.Consume()
		if err != nil {
			fmt.Println("Error while consuming audio: ", err)
			return
		}
		audioBuffer = append(audioBuffer, message.Value...)
		audioBufferStartTime = message.Time.Add(-time.Second)

		for {
			message, err = audioConsumer.Consume()
			if err != nil {
				fmt.Println("Error while consuming audio: ", err)
				return
			}
			audioBuffer = append(audioBuffer, message.Value...)
		}
	}()

	go func() {
		for {
			message, err := inputConsumer.Consume()
			if err != nil {
				fmt.Println("Error while consuming input: ", err)
				return
			}
			inputBuffer = append(inputBuffer, message)
		}
	}()

	for {
		time.Sleep(2 * time.Second)
		oneSecondOfAudio := audioBuffer[:44100]
		oneSecondOfVideo := make([][]byte, 0)

		var imagesConsumed int
		var imageMessage kafka.Message
		for imagesConsumed, imageMessage = range videoBuffer {
			if imageMessage.Time.After(audioBufferStartTime.Add(time.Second)) {
				break
			}

			oneSecondOfVideo = append(oneSecondOfVideo, imageMessage.Value)
		}

		fmt.Println(len(oneSecondOfVideo), len(oneSecondOfAudio))

		audioBuffer = audioBuffer[44100:]
		audioBufferStartTime = audioBufferStartTime.Add(time.Second)
		videoBuffer = videoBuffer[imagesConsumed:]
	}
}
