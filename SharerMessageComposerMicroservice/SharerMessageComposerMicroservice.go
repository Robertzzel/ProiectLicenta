package main

import (
	"Licenta/kafka"
	"fmt"
	"os"
	"os/exec"
	"time"
)

const (
	kafkaImagesTopic   = "video"
	kafkaAudioTopic    = "audio"
	kafkaMessagesTopic = "messages"
	kafkaSyncTopic     = "sync"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

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

func combineVideoAndAudioFiles(videoFileName, audioFileName string) (string, []byte, error) {
	outputFile, err := os.CreateTemp("", "*out.mp4")
	if err != nil {
		return "", nil, err
	}

	if _, err := exec.Command("./CombineAndCompress", videoFileName, audioFileName, outputFile.Name(), "30").Output(); err != nil {
		return "", nil, err
	}

	fileBytes, err := os.ReadFile(outputFile.Name())
	if err != nil {
		return "", nil, err
	}

	return outputFile.Name(), fileBytes, nil
}

func main() {
	checkErr(createKafkaTopics())

	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	videoConsumer := kafka.NewKafkaConsumer(kafkaImagesTopic)
	syncConsumer := kafka.NewKafkaConsumer(kafkaSyncTopic)
	interAppProducer := kafka.NewInterAppProducer(kafkaMessagesTopic)

	checkErr(syncConsumer.SetOffsetToNow())
	checkErr(audioConsumer.SetOffsetToNow())
	checkErr(videoConsumer.SetOffsetToNow())

	for {
		videoMessage, err := videoConsumer.Consume()
		checkErr(err)

		audioMessage, err := audioConsumer.Consume()
		checkErr(err)

		go func() {
			s := time.Now()
			videoFileName := string(videoMessage.Value)
			audioFileName := string(audioMessage.Value)

			fmt.Println(videoFileName, audioFileName)

			fileName, fileBytes, err := combineVideoAndAudioFiles(videoFileName, audioFileName)
			checkErr(err)

			checkErr(interAppProducer.Publish(fileBytes))
			fmt.Println("New File: ", len(fileBytes), time.Since(s))

			checkErr(os.Remove(videoFileName))
			checkErr(os.Remove(audioFileName))
			checkErr(os.Remove(fileName))
		}()
	}
}
