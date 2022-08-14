package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
)

const (
	kafkaImagesTopic   = "video"
	kafkaAudioTopic    = "audio"
	kafkaMessagesTopic = "messages"
	kafkaSyncTopic     = "sync"
	outputFileName     = "output.mp4"
	outputVideoFile    = "video.avi"
	outputAudioFile    = "audio.wav"
)

func createKafkaTopics() error {
	err := kafka.CreateTopic(kafkaImagesTopic)
	if err != nil {
		return err
	}
	err = kafka.CreateTopic(kafkaAudioTopic)
	if err != nil {
		return err
	}
	return kafka.CreateTopic(kafkaMessagesTopic)
}

func main() {
	err := createKafkaTopics()
	if err != nil {
		fmt.Println(err)
	}

	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	videoConsumer := kafka.NewKafkaConsumer(kafkaImagesTopic)
	syncConsumer := kafka.NewKafkaConsumer(kafkaSyncTopic)
	err = syncConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	err = audioConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	err = videoConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	interAppProducer := kafka.NewInterAppProducer(kafkaMessagesTopic)

	var wg sync.WaitGroup
	for {
		wg.Add(2)

		go func(waitG *sync.WaitGroup) {
			_, err = audioConsumer.Consume()
			if err != nil {
				return
			}
			wg.Done()
		}(&wg)

		go func(waitG *sync.WaitGroup) {
			_, err = videoConsumer.Consume()
			if err != nil {
				return
			}
			wg.Done()
		}(&wg)
		wg.Wait()

		go func() {
			err = exec.Command("./CombineAudioAndVideo", outputVideoFile, outputAudioFile, outputFileName).Run()
			if err != nil {
				fmt.Println(err)
				return
			}

			file, err := os.ReadFile(outputFileName)
			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("New File: ", len(file))

			err = interAppProducer.Publish(file)
			if err != nil {
				fmt.Println(err)
				return
			}
		}()
	}
}
