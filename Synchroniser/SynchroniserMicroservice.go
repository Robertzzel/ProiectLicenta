package main

import (
	"Licenta/kafka"
	"fmt"
	"sync"
)

const (
	topic      = "sync"
	videoTopic = "videoSync"
	audioSync  = "audioSync"
)

func main() {
	if err := kafka.CreateTopic(topic); err != nil {
		fmt.Println(err)
		return
	}

	producer := kafka.NewSyncKafkaProducer(topic)
	videoSyncConsumer := kafka.NewKafkaConsumer(videoTopic)
	audioSyncConsumer := kafka.NewKafkaConsumer(audioSync)

	if err := videoSyncConsumer.SetOffsetToNow(); err != nil {
		panic(err)
	}
	if err := audioSyncConsumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for {

		wg.Add(2)
		go func() {
			fmt.Println("w for vid")
			_, err := videoSyncConsumer.Consume()
			if err != nil {
				panic(err)
			}
			fmt.Println("vid key")
			wg.Done()
		}()

		go func() {
			fmt.Println("w for aud")
			_, err := audioSyncConsumer.Consume()
			if err != nil {
				panic(err)
			}
			fmt.Println("aud key")
			wg.Done()
		}()

		wg.Wait()
		if err := producer.Publish([]byte(".")); err != nil {
			panic(err)
		}

		fmt.Println("Sync")
	}
}
