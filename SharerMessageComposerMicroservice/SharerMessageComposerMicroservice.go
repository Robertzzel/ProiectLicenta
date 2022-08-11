package main

import (
	"Licenta/kafka"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	kafkaImagesTopic    = "video"
	kafkaAudioTopic     = "audio"
	kafkaInputTopic     = "input"
	kafkaMessagesTopic  = "messages"
	kafkaSyncTopic      = "sync"
	audioRecordInterval = time.Second
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

	//
	var buffer *bytes.Buffer = bytes.NewBuffer(make([]byte, 0))
	encoder := json.NewEncoder(buffer)

	listener, err := net.Listen("tcp", "localhost: 8081")
	if err != nil {
		return
	}

	fmt.Println("AStept conexiune")
	conn, err := listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	fmt.Println("Conexiune stabilita")

	//

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

	syncMessage, err := audioConsumer.Consume()
	if err != nil {
		fmt.Println("Sytnc error: ", err)
		return
	}
	lastAudioReceivedTime, _ := time.Parse(kafka.TimeFormat, string(syncMessage.Headers[0].Value))

	var waitGroup sync.WaitGroup
	for {
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
		messageToBeSent.Timestamp = time.Now()

		buffer.Truncate(0)
		err := encoder.Encode(messageToBeSent)
		if err != nil {
			return
		}

		if buffer.Len() < 10 {
			continue
		}

		sizeString := fmt.Sprintf("%09d", buffer.Len())

		_, err = conn.Write([]byte(sizeString))
		if err != nil {
			return
		}

		_, err = conn.Write(buffer.Bytes())
		if err != nil {
			return
		}
	}
}
