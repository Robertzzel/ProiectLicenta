package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"os"
	"time"
)

const (
	interAppMessagesTopic = "messages"
	receivedImagesTopic   = "rImages"
	receivedAudioTopic    = "rAudio"
	FPS                   = 1.0 / 3
	interImageDuration    = time.Duration(float32(time.Second) * FPS / (30 * FPS))
	fileName              = "image.png"
)

func main() {
	interAppConsumer := kafka.NewInterAppConsumer(interAppMessagesTopic)
	//receivedImagesProducer := kafka.NewImageKafkaProducer(receivedImagesTopic)
	//receivedAudioProducer := kafka.NewImageKafkaProducer(receivedAudioTopic)

	err := interAppConsumer.Reader.SetOffsetAt(context.Background(), time.Now().Add(time.Second))
	if err != nil {
		return
	}
	file, _ := os.Create(fileName)

	for {
		message, err := interAppConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println(len(message.Audio), len(message.Images), message.Timestamp)

		//receivedAudioProducer.Publish(message.Audio)
		go func(images [][]byte) {
			for _, image := range images {
				//receivedImagesProducer.Publish(image)
				file.Truncate(0)
				file.Seek(0, 0)
				file.Write(image)
				fmt.Print("_ ")
				time.Sleep(interImageDuration)
			}
			fmt.Println("")
		}(message.Images)
	}
}
