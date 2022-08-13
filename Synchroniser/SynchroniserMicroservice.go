package main

import (
	"Licenta/kafka"
	"fmt"
	"time"
)

const (
	topic                  = "sync"
	intervalBetweenSignals = time.Second * 2
)

func main() {
	fmt.Println("Starting...")

	err := kafka.CreateTopic(topic)
	if err != nil {
		fmt.Println(err)
		return
	}
	producer := kafka.NewSyncKafkaProducer(topic)
	i := 0
	for {
		s := time.Now()

		err := producer.PublishWithTimestamp([]byte(fmt.Sprint(i)))
		if err != nil {
			fmt.Println(err)
			return
		}

		i++
		fmt.Print("signal ", i, " ")
		time.Sleep(intervalBetweenSignals - time.Since(s))
	}
}
