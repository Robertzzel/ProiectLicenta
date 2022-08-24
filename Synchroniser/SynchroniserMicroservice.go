package main

import (
	"Licenta/kafka"
	"fmt"
	"time"
)

const (
	topic                  = "sync"
	intervalBetweenSignals = time.Second
)

func main() {
	if err := kafka.CreateTopic(topic); err != nil {
		fmt.Println(err)
		return
	}
	producer := kafka.NewSyncKafkaProducer(topic)

	for {
		s := time.Now()

		if err := producer.PublishWithTimestamp([]byte(".")); err != nil {
			fmt.Println(err)
			return
		}

		fmt.Print("signal ")
		time.Sleep(intervalBetweenSignals - time.Since(s))
	}
}
