package main

import (
	"Licenta/kafka"
	"fmt"
	"time"
)

const (
	kafkaTopic = "video"
	syncTopic  = "sync"
	syncTopic2 = "videoSync"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func synchronise(syncPublisher *kafka.Producer, syncConsumer *kafka.Consumer) error {
	if err := syncPublisher.Publish([]byte(".")); err != nil {
		return err
	}

	_, err := syncConsumer.Consume()
	if err != nil {
		return err
	}

	fmt.Println("sync")
	return nil
}

func main() {
	checkErr(kafka.CreateTopic(kafkaTopic))

	videoPublisher := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncPublisher := kafka.NewSyncKafkaProducer(syncTopic2)
	syncConsumer := kafka.NewKafkaConsumer(syncTopic)

	checkErr(syncConsumer.SetOffsetToNow())

	service, err := NewVideoGenerator()
	checkErr(err)

	checkErr(synchronise(syncPublisher, syncConsumer))

	messagesUntilSync := 30
	for {
		fileName := "videos/" + fmt.Sprint(time.Now().Unix()) + ".mkv"
		checkErr(service.GenerateVideoFile(fileName, time.Second))
		checkErr(videoPublisher.Publish([]byte(fileName)))

		messagesUntilSync--
		if messagesUntilSync == 0 {
			checkErr(synchronise(syncPublisher, syncConsumer))
			messagesUntilSync = 30
		}
		fmt.Println(time.Now())
	}

}
