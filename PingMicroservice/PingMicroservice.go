package main

import (
	"Licenta/Kafka"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const (
	StreamerTopic = "StreamerPing"
	ReceiverTopic = "ReceiverPing"
)

var quit = make(chan os.Signal, 2)

func checkErr(err error) {
	if err != nil {
		quit <- syscall.SIGINT
	}
}

func Abs(nr int) int {
	if nr > 0 {
		return nr
	}
	return -nr
}

func main() {
	if err := Kafka.CreateTopic(ReceiverTopic, 1); err != nil {
		panic(err)
	}

	if err := Kafka.CreateTopic(StreamerTopic, 1); err != nil {
		panic(err)
	}

	receiverConsumer := Kafka.NewConsumer(ReceiverTopic, 0)
	streamerConsumer := Kafka.NewConsumer(StreamerTopic, 0)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("Starting cleanup")
		Kafka.DeleteTopic(ReceiverTopic)
		Kafka.DeleteTopic(StreamerTopic)
		streamerConsumer.Close()
		receiverConsumer.Close()
		fmt.Println("Cleanup done")
		os.Exit(1)
	}()

	for {
		receiverMessage, err := receiverConsumer.Consume()
		checkErr(err)
		streamerMessage, err := streamerConsumer.Consume()
		checkErr(err)

		tsReceiver, err := strconv.Atoi(string(receiverMessage.Message))
		checkErr(err)

		tsStreamer, err := strconv.Atoi(string(streamerMessage.Message))
		checkErr(err)

		fmt.Println(Abs(tsReceiver - tsStreamer))
	}
}
