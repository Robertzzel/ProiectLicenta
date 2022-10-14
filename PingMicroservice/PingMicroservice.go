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

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Abs(nr int) int {
	if nr > 0 {
		return nr
	}
	return -nr
}

func main() {
	checkErr(Kafka.CreateTopic(ReceiverTopic))
	defer Kafka.DeleteTopic(ReceiverTopic)

	checkErr(Kafka.CreateTopic(StreamerTopic))
	defer Kafka.DeleteTopic(StreamerTopic)

	receiverConsumer := Kafka.NewConsumer(ReceiverTopic)
	defer receiverConsumer.Close()

	streamerConsumer := Kafka.NewConsumer(StreamerTopic)
	defer streamerConsumer.Close()

	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("Starting cleanup")
		Kafka.DeleteTopic(ReceiverTopic)
		Kafka.DeleteTopic(StreamerTopic)
		fmt.Println("Cleanup done")
		os.Exit(1)
	}()

	for {
		msgStreamer, err := receiverConsumer.Consume()
		checkErr(err)

		msgReceiver, err := streamerConsumer.Consume()
		checkErr(err)

		tsReceiver, err := strconv.Atoi(string(msgReceiver.Value))
		checkErr(err)

		tsStreamer, err := strconv.Atoi(string(msgStreamer.Value))
		checkErr(err)

		fmt.Println(Abs(tsReceiver - tsStreamer))
	}
}
