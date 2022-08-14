package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"os"
	"time"
)

const (
	receivedImagesTopic = "rImages"
	receivedAudioTopic  = "rAudio"
	interAppTopic       = "messages"
)

func main() {
	//receivedImagesProducer := kafka.NewImageKafkaProducer(receivedImagesTopic)
	//receivedAudioProducer := kafka.NewImageKafkaProducer(receivedAudioTopic)
	interAppConsumer := kafka.NewKafkaConsumer(interAppTopic)
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

		file, err := os.Create("received2")
		if err != nil {
			return
		}

		file.Truncate(0)
		file.Seek(0, 0)
		file.Write(message.Value)
		return
	}
}
