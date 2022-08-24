package main

import (
	"Licenta/kafka"
	"fmt"
	"sync"
	"time"
)

const (
	kafkaTopic         = "video"
	syncTopic          = "sync"
	FPS                = 30
	resizedImageWidth  = 1280
	resizedImageHeight = 720
	videoSize          = time.Second
	compressQuality    = 100
)

func main() {
	if err := kafka.CreateTopic(kafkaTopic); err != nil {
		fmt.Println(err)
		return
	}
	kafkaProducer := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncConsumer := kafka.NewKafkaConsumer(syncTopic)
	if err := syncConsumer.SetOffsetToNow(); err != nil {
		fmt.Println(err)
		return
	}

	service, err := NewVideoGenerator()
	if err != nil {
		fmt.Print(err)
		return
	}

	var mutex sync.Mutex
	for {
		syncMsg, err := syncConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(string(syncMsg.Value), time.Now())

		go func() {
			mutex.Lock()
			videoFileName, err := service.GenerateVideoFile(videoSize)
			if err != nil {
				fmt.Println(err)
				return
			}
			mutex.Unlock()

			err = kafkaProducer.Publish([]byte(videoFileName))
			if err != nil {
				fmt.Println(err)
				return
			}
		}()
	}
}
