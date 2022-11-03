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
	VideoDuration = time.Second / 2
	VideoTopic    = "video"
)

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

	timestamp, err := stringToTimestamp(os.Args[1])
	if err != nil {
		log.Fatal("Timestamp not valid")
	}

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	videoRecorder, err := NewRecorder(30)
	if err != nil {
		log.Fatal("Recorder cannot be initiated: ", err)
	}
	videoRecorder.Start(timestamp, VideoDuration)

	if err := Kafka.CreateTopic(VideoTopic); err != nil {
		panic(err)
	}

	composer := Kafka.NewProducer()
	defer func() {
		composer.Close()
		Kafka.DeleteTopic(VideoTopic)
		fmt.Println("Producer closed")
	}()

	for {
		select {
		case videoName := <-videoRecorder.VideoBuffer:
			if err := composer.Publish(&Kafka.ProducerMessage{Message: []byte(videoName), Topic: VideoTopic}); err != nil {
				panic(err)
			}
			log.Println(videoName, time.Now().UnixMilli())
		case <-quit:
			fmt.Println("Quit signal")
			return
		}
	}
}
