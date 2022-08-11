package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"os"
	"time"
)

const (
	imagesTopic = "rImages"
	fileName    = "image.png"
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

		fmt.Println("___", image.Time)
		file.Truncate(0)
		file.Seek(0, 0)
		file.Write(image.Value)
		fmt.Println(time.Since(s))

	}
}
