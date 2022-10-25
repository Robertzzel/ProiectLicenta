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
	receiverConsumer, err := Kafka.NewConsumer(ReceiverTopic)
	checkErr(err)

	streamerConsumer, err := Kafka.NewConsumer(StreamerTopic)
	checkErr(err)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("Starting cleanup")
		streamerConsumer.Close()
		receiverConsumer.Close()
		Kafka.DeleteTopic(ReceiverTopic)
		Kafka.DeleteTopic(StreamerTopic)
		fmt.Println("Cleanup done")
		os.Exit(1)
	}()

	for {
		receiverMessage, err := receiverConsumer.Consume()
		checkErr(err)
		streamerMessage, err := streamerConsumer.Consume()
		checkErr(err)

		tsReceiver, err := strconv.Atoi(string(receiverMessage))
		checkErr(err)

		tsStreamer, err := strconv.Atoi(string(streamerMessage))
		checkErr(err)

		fmt.Println(Abs(tsReceiver - tsStreamer))
	}
}
