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

func combineVideoAndAudioFiles(videoFileName, audioFileName string) (string, error) {
	outputFile, err := os.CreateTemp("", "*out.mp4")
	if err != nil {
		return "", err
	}

	if _, err := exec.Command("./CombineAndCompress", videoFileName, audioFileName, outputFile.Name(), "30").Output(); err != nil {
		return "", err
	}

	return outputFile.Name(), nil
}

func main() {
	checkErr(createKafkaTopics())

	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	videoConsumer := kafka.NewKafkaConsumer(kafkaImagesTopic)
	routerProducer := kafka.NewInterAppProducer(kafkaMessagesTopic)

	checkErr(audioConsumer.SetOffsetToNow())
	checkErr(videoConsumer.SetOffsetToNow())

	for {
		videoMessage, err := videoConsumer.Consume()
		checkErr(err)

		audioMessage, err := audioConsumer.Consume()
		checkErr(err)

		go func() {
			videoFileName := string(videoMessage.Value)
			audioFileName := string(audioMessage.Value)

			s := time.Now()
			fileName, err := combineVideoAndAudioFiles(videoFileName, audioFileName)
			checkErr(err)
			fmt.Println(time.Since(s))

			checkErr(routerProducer.Publish([]byte(fileName)))

			checkErr(os.Remove(videoFileName))
			checkErr(os.Remove(audioFileName))
		}()
	}
}
