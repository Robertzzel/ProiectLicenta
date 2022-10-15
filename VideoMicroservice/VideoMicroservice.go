package main

import (
	"Licenta/Kafka"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	VideoDuration = time.Second
	VideoTopic    = "video"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func stringToTimestamp(s string) (time.Time, error) {
	timestamp, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(timestamp, 0), nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("No timestamp given")
	}

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	timestamp, err := stringToTimestamp(os.Args[1])
	checkErr(err)

	videoRecorder, err := NewRecorder(24)
	checkErr(err)
	videoRecorder.Start(timestamp, VideoDuration)

	checkErr(Kafka.CreateTopic(VideoTopic))
	defer func() {
		Kafka.DeleteTopic(VideoTopic)
		fmt.Println("Topic Deleted")
	}()

	composer := Kafka.NewProducerAsync(VideoTopic)
	defer func() {
		composer.Close()
		fmt.Println("Producer closed")
	}()

mainFor:
	for {
		select {
		case videoName := <-videoRecorder.VideoBuffer:
			checkErr(composer.Publish([]byte(videoName)))
			fmt.Println(videoName, "sent at ", time.Now().UnixMilli())
		case <-quit:
			fmt.Println("Quit signal")
			break mainFor
		}
	}
}
