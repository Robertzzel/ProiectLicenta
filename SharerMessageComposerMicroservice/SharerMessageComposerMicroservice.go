package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	kafkaImagesTopic    = "video"
	kafkaAudioTopic     = "audio"
	kafkaInputTopic     = "input"
	kafkaMessagesTopic  = "messages"
	kafkaSyncTopic      = "sync"
	audioRecordInterval = time.Second / 6
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
	err = kafka.CreateTopic(kafkaInputTopic)
	if err != nil {
		return err
	}
	err = kafka.CreateTopic(kafkaMessagesTopic)
	if err != nil {
		return err
	}
	err = kafka.CreateTopic(kafkaSyncTopic)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	err := createKafkaTopics()
	if err != nil {
		fmt.Println(err)
	}

	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	videoConsumer := kafka.NewKafkaConsumer(kafkaImagesTopic)
	inputConsumer := kafka.NewKafkaConsumer(kafkaInputTopic)
	syncConsumer := kafka.NewKafkaConsumer(kafkaSyncTopic)
	//interAppMessagesProducer := kafka.NewInterAppProducer(kafkaMessagesTopic)

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
	err = inputConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	err = syncConsumer.Reader.SetOffsetAt(context.Background(), time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}

	syncMessage, err := audioConsumer.Consume()
	if err != nil {
		fmt.Println("Sytnc error: ", err)
		return
	}
	lastAudioReceivedTime, _ := time.Parse(kafka.TimeFormat, string(syncMessage.Headers[0].Value))

	var waitGroup sync.WaitGroup
	for {
		s := time.Now()
		messageToBeSent := kafka.InterAppMessage{}
		waitGroup.Add(2)

		go func(wg *sync.WaitGroup) {
			audioMessage, err := audioConsumer.Consume()
			if err != nil {
				fmt.Println("Error while consuming audio: ", err)
				return
			}
			messageToBeSent.Audio = audioMessage.Value

			lastAudioReceivedTime, _ = time.Parse(kafka.TimeFormat, string(audioMessage.Headers[0].Value))
			waitGroup.Done()
		}(&waitGroup)

		go func(wg *sync.WaitGroup, lowerTimeLimit time.Time) {
			upperTimeLimit := lowerTimeLimit.Add(audioRecordInterval)
			for {
				imageMessage, err := videoConsumer.Consume()
				if err != nil {
					fmt.Println("Eroare la primirea imaginii ", err)
					return
				}

				timestamp, err := time.Parse(kafka.TimeFormat, string(imageMessage.Headers[0].Value))
				if err != nil {
					fmt.Println("Eroare la timpstamp la imagine", err)
				}

				if timestamp.Before(lowerTimeLimit) {
					continue
				}

				messageToBeSent.Images = append(messageToBeSent.Images, imageMessage.Value)
				if timestamp.After(upperTimeLimit) {
					break
				}
			}
			wg.Done()
		}(&waitGroup, lastAudioReceivedTime)

		waitGroup.Wait()

		fmt.Println(len(messageToBeSent.Images), len(messageToBeSent.Audio), time.Since(s))
		//err := interAppMessagesProducer.Publish(messageToBeSent)
		//if err != nil {
		//	fmt.Println("Encoding error", err)
		//	return
		//}
	}
}
