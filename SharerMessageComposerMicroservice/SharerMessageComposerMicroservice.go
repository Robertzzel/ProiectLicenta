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
	outputFileName     = "output.mp4"
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

func combineVideoAndAudioFiles(videoFileName, audioFileName string) ([]byte, error) {

	/*
		if _, err := exec.Command("./CombineAudioAndVideo", videoFileName, audioFileName, "aux"+outputFileName).Output(); err != nil {
			return nil, err
		}

		if _, err := exec.Command("./CompressFile", "aux"+outputFileName, outputFileName, "30").Output(); err != nil {
			return nil, err
		}*/

	if _, err := exec.Command("./CombineAndCompress", videoFileName, audioFileName, outputFileName, "30").Output(); err != nil {
		return nil, err
	}

	return os.ReadFile(outputFileName)
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

			file, err := combineVideoAndAudioFiles(videoFileName, audioFileName)
			checkErr(err)

			checkErr(interAppProducer.Publish(file))
			fmt.Println("New File: ", len(file), time.Since(s))

			checkErr(os.Remove(videoFileName))
			checkErr(os.Remove(audioFileName))
		}()
	}
}
