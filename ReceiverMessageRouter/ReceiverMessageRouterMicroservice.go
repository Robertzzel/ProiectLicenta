package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"time"
)

const (
	receivedImagesTopic = "rImages"
	receivedAudioTopic  = "rAudio"
	interAppTopic       = "messages"
)

func main() {
	receivedImagesProducer := kafka.NewImageKafkaProducer(receivedImagesTopic)
	receivedAudioProducer := kafka.NewImageKafkaProducer(receivedAudioTopic)
	interAppConsumer := kafka.NewInterAppConsumer(interAppTopic)
	err := interAppConsumer.Reader.SetOffsetAt(context.Background(), time.Now().Add(time.Hour))
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		message, err := interAppConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println(len(message.Images), len(message.Audio), message.Timestamp, time.Now())
		receivedAudioProducer.Publish(message.Audio)
		for _, image := range message.Images {
			receivedImagesProducer.Publish(image)
		}
	}
}
