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
	if err := kafka.CreateTopic(kafkaTopic); err != nil {
		panic(err)
	}

	videoPublisher := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncPublisher := kafka.NewSyncKafkaProducer(syncTopic2)

	syncConsumer := kafka.NewKafkaConsumer(syncTopic)
	if err := syncConsumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	service, err := NewVideoGenerator()
	if err != nil {
		panic(err)
	}

	if err := synchronise(syncPublisher, syncConsumer); err != nil {
		panic(err)
	}

	messagesUntilSync := 30
	for {
		fileName := "videos/" + fmt.Sprint(time.Now().Unix()) + ".mkv"
		if err = service.GenerateVideoFile(fileName, time.Second); err != nil {
			panic(err)
		}

		if err = videoPublisher.Publish([]byte(fileName)); err != nil {
			panic(err)
		}

		messagesUntilSync--
		if messagesUntilSync == 0 {

			if err := synchronise(syncPublisher, syncConsumer); err != nil {
				panic(err)
			}

			messagesUntilSync = 30
		}
		fmt.Println(time.Now())
	}

}
