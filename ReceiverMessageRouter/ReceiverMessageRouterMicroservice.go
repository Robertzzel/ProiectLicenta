package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"os"
	"time"
)

const (
	interAppTopic = "messages"
)

func main() {
	interAppConsumer := kafka.NewKafkaConsumer(interAppTopic)

	if err := interAppConsumer.Reader.SetOffsetAt(context.Background(), time.Now().Add(time.Hour)); err != nil {
		fmt.Println(err)
		return
	}

	file, err := os.Create("out")
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

		file.Truncate(0)
		file.Seek(0, 0)
		file.Write(message.Value)
	}
}
