package main

import (
	"Licenta/kafka"
	"fmt"
	"log"
	"strconv"
	"time"
)

const (
	kafkaTopic   = "video"
	syncTopic    = "sync"
	syncTopic2   = "videoSync"
	syncInterval = 60
	videoSize    = time.Second
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func synchronise(syncPublisher *kafka.Producer, syncConsumer *kafka.Consumer) (time.Time, error) {
	if err := syncPublisher.Publish([]byte(".")); err != nil {
		return time.Time{}, err
	}

	syncMsg, err := syncConsumer.Consume()
	if err != nil {
		return time.Now(), err
	}

	timestamp, err := strconv.ParseInt(string(syncMsg.Value), 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(timestamp, 0), nil
}

func main() {
	checkErr(kafka.CreateTopic(kafkaTopic))
	videoPublisher := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncPublisher := kafka.NewSyncKafkaProducer(syncTopic2)
	syncConsumer := kafka.NewKafkaConsumer(syncTopic)
	checkErr(syncConsumer.SetOffsetToNow())

	videoRecorder, err := NewRecorder(30)
	checkErr(err)
	videoRecorder.Start()

	startTime, err := synchronise(syncPublisher, syncConsumer)
	checkErr(err)
	log.Println("syncked")

	for {
		for i := 0; i < syncInterval; i++ {
			partStartTime := startTime.Add(time.Duration(int64(videoSize) * int64(i)))
			fileName := "videos/" + fmt.Sprint(partStartTime.Unix()) + ".mkv"

			checkErr(videoRecorder.CreateFile(fileName, partStartTime, videoSize))
			checkErr(videoPublisher.Publish([]byte(fileName)))

			log.Println("video", fileName, partStartTime.Unix())
			fmt.Println(" ")
		}

		startTime, err = synchronise(syncPublisher, syncConsumer)
		checkErr(err)
	}
}
