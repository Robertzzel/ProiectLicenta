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

func combineVideoAndAudioFiles(videoFileBytes, audioFileBytes []byte) ([]byte, error) {
	videoTemp, err := os.CreateTemp("", "temp")
	if err != nil {
		return nil, err
	}
	defer videoTemp.Close()

	if _, err = videoTemp.Write(videoFileBytes); err != nil {
		return nil, err
	}

	audioTemp, err := os.CreateTemp("", "temp")
	if err != nil {
		return nil, err
	}
	defer audioTemp.Close()

	if _, err = audioTemp.Write(audioFileBytes); err != nil {
		return nil, err
	}

	if err = exec.Command("./CombineAudioAndVideo", videoTemp.Name(), audioTemp.Name(), outputFileName).Run(); err != nil {
		return nil, err
	}

	file, err := os.ReadFile(outputFileName)
	if err != nil {
		return nil, err
	}

	return file, nil
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
			return
		}

		audioMessage, err := audioConsumer.Consume()
		if err != nil {
			return
		}

		go func() {
			file, err := combineVideoAndAudioFiles(videoMessage.Value, audioMessage.Value)
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
