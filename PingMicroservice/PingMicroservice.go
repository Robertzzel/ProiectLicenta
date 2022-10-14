package main

import (
	"Licenta/Kafka"
	"fmt"
	"strconv"
)

const (
	StreamerTopic = "StreamerPing"
	ReceiverTopic = "ReceiverPing"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Abs(nr int64) int64 {
	if nr > 0 {
		return nr
	}
	return -nr
}

func main() {
	streamerPings := make(chan int64, 10)
	receiverPings := make(chan int64, 10)

	go func() {
		checkErr(Kafka.CreateTopic(StreamerTopic))
		defer Kafka.DeleteTopic(StreamerTopic)

		producer := Kafka.NewConsumer(StreamerTopic)
		defer producer.Close()

		msg, err := producer.Consume()
		checkErr(err)

		timestamp, err := strconv.Atoi(string(msg.Value))
		checkErr(err)

		streamerPings <- int64(timestamp)
	}()

	go func() {
		checkErr(Kafka.CreateTopic(ReceiverTopic))
		defer Kafka.DeleteTopic(ReceiverTopic)

		producer := Kafka.NewConsumer(ReceiverTopic)
		defer producer.Close()

		msg, err := producer.Consume()
		checkErr(err)

		timestamp, err := strconv.Atoi(string(msg.Value))
		checkErr(err)

		receiverPings <- int64(timestamp)
	}()

	for {
		fmt.Println(Abs(<-streamerPings - <-receiverPings))
	}
}
