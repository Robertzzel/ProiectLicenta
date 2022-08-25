package main

import (
	"Licenta/kafka"
	"fmt"
	"os"
	"os/exec"
)

const (
	kafkaImagesTopic   = "video"
	kafkaAudioTopic    = "audio"
	kafkaMessagesTopic = "messages"
	kafkaSyncTopic     = "sync"
	outputFileName     = "output.mp4"
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

func combineVideoAndAudioFiles(videoFileName, audioFileName string) ([]byte, error) {
	if _, err := exec.Command("./CombineAudioAndVideo", videoFileName, audioFileName, "aux"+outputFileName).Output(); err != nil {
		return nil, err
	}

	if _, err := exec.Command("./CompressFile", "aux"+outputFileName, outputFileName, "30").Output(); err != nil {
		return nil, err
	}

	return os.ReadFile(outputFileName)
}

func main() {
	if err := createKafkaTopics(); err != nil {
		fmt.Println(err)
	}

	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	videoConsumer := kafka.NewKafkaConsumer(kafkaImagesTopic)
	syncConsumer := kafka.NewKafkaConsumer(kafkaSyncTopic)

	if err := syncConsumer.SetOffsetToNow(); err != nil {
		fmt.Println(err)
		return
	}

	if err := audioConsumer.SetOffsetToNow(); err != nil {
		fmt.Println(err)
		return
	}

	if err := videoConsumer.SetOffsetToNow(); err != nil {
		fmt.Println(err)
		return
	}
	interAppProducer := kafka.NewInterAppProducer(kafkaMessagesTopic)

	for {
		videoMessage, err := videoConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			return
		}

		audioMessage, err := audioConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			return
		}

		go func() {
			videoFileName := string(videoMessage.Value)
			audioFileName := string(audioMessage.Value)

			file, err := combineVideoAndAudioFiles(videoFileName, audioFileName)
			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("New File: ", len(file))
			if err = interAppProducer.Publish(file); err != nil {
				fmt.Println(err)
				return
			}

			if err = os.Remove(videoFileName); err != nil {
				fmt.Println("Error: ", err)
				return
			}

			if err = os.Remove(audioFileName); err != nil {
				fmt.Println("Error: ", err)
				return
			}
		}()
	}
}
