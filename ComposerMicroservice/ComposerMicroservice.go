package main

import (
	"Licenta/kafka"
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

func processFiles(videoFileName, audioFileName string) (string, error) {
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

	videoFiles := make(chan string, 10)
	audioFiles := make(chan string, 10)
	errors := make(chan error, 10)

	go func() {
		for {
			videoMessage, err := videoConsumer.Consume()
			if err != nil {
				errors <- err
				break
			}

			videoFiles <- string(videoMessage.Value)
		}
	}()

	go func() {
		for {
			audioMessage, err := audioConsumer.Consume()
			if err != nil {
				errors <- err
				break
			}

			audioFiles <- string(audioMessage.Value)
		}
	}()

	for {
		go func(videoFile, audioFile string) {
			println("Primit", videoFile, " la ", time.Now())
			fileName, err := processFiles(videoFile, audioFile)
			checkErr(err)

			checkErr(routerProducer.Publish([]byte(fileName)))
			println("Trimis", videoFile, "la", time.Now(), "\n")

			checkErr(os.Remove(videoFile))
			checkErr(os.Remove(audioFile))
		}(<-videoFiles, <-audioFiles)
	}
}
