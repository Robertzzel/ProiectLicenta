package main

import (
	"Licenta/Kafka"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const (
	StreamerTopic = "StreamerPing"
	ReceiverTopic = "ReceiverPing"
)

func checkErr(err error, msg string) {
	if err != nil {
		log.Println(msg)
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
	receiverConsumer, err := Kafka.NewConsumer(ReceiverTopic)
	checkErr(err, "Error while creating receiver consumer")

	streamerConsumer, err := Kafka.NewConsumer(StreamerTopic)
	checkErr(err, "Error while creating streamer consumer")

	quit := make(chan os.Signal, 2)
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
		tsReceiver, err := strconv.Atoi(string(receiverConsumer.Consume()))
		checkErr(err, "Error while parsing timestamp from receiver")

		tsStreamer, err := strconv.Atoi(string(streamerConsumer.Consume()))
		checkErr(err, "Error while parsing timestamp from streamer")

		fmt.Println(Abs(tsReceiver - tsStreamer))
	}
}
