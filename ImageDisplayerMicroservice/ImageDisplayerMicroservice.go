package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"os"
	"time"
)

const (
	imagesTopic        = "rImages"
	fileName           = "image.png"
	interImageDuration = time.Second / 30
)

func main() {
	imagesConsumer := kafka.NewKafkaConsumer(imagesTopic)
	imagesConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	file, _ := os.Create(fileName)

	for {
		s := time.Now()
		image, err := imagesConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			break
		}

		file.Truncate(0)
		file.Seek(0, 0)
		file.Write(image.Value)
		time.Sleep(interImageDuration - time.Since(s))
		fmt.Print("_ ")
	}
}
