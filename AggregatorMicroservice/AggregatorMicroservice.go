package main

import (
	"Licenta/Kafka"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	VideoTopic    = "video"
	AudioTopic    = "audio"
	ComposerTopic = "aggregator"
	StreamerTopic = "StreamerPing"
)

type AudioVideoPair struct {
	Video string
	Audio string
}

func checkErr(err error, message string) {
	if err != nil {
		log.Println(message)
		panic(err)
	}
}

func getSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer, outputChannel chan AudioVideoPair) {
	for {
		videoFile := string(videoConsumer.Consume())
		audioFile := string(audioConsumer.Consume())

		videoTimestamp, err := strconv.Atoi(videoFile[len(videoFile)-17 : len(videoFile)-4])
		if err != nil {
			log.Println("Error, video timestamp not valid")
			continue
		}
		audioTimestamp, err := strconv.Atoi(audioFile[len(audioFile)-17 : len(audioFile)-4])
		if err != nil {
			log.Println("Error, audio timestamp not valid")
			continue
		}

		timestampDifference := (videoTimestamp - audioTimestamp) / 500
		if timestampDifference > 0 {
			log.Println("Desync ", "audio by ", timestampDifference)
			for i := 0; i < timestampDifference; i++ {
				audioFile = string(audioConsumer.Consume())
			}
		} else if timestampDifference < 0 {
			log.Println("Desync ", "video by ", timestampDifference)
			for i := 0; i < -timestampDifference; i++ {
				videoFile = string(videoConsumer.Consume())
			}
		}

		outputChannel <- AudioVideoPair{Video: videoFile, Audio: audioFile}
	}
}

func main() {
	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	videoConsumer, err := Kafka.NewConsumer(VideoTopic)
	checkErr(err, "Error while crating video consumer")
	defer func() {
		videoConsumer.Close()
		log.Println("video consumer closed")
	}()

	audioConsumer, err := Kafka.NewConsumer(AudioTopic)
	checkErr(err, "Error while crating audio consumer")
	defer func() {
		audioConsumer.Close()
		log.Println("video consumer closed")
	}()

	producer, err := Kafka.NewProducer()
	defer func() {
		producer.Close()
		Kafka.DeleteTopic(ComposerTopic)
		log.Println("producer and topic closed")
	}()

	filesChannel := make(chan AudioVideoPair, 5)
	go getSyncedAudioAndVideo(videoConsumer, audioConsumer, filesChannel)

	for {
		select {
		case files := <-filesChannel:
			video, err := exec.Command("./CombineAndCompress", files.Video, files.Audio, "2046k").Output()
			if err != nil {
				var exitError *exec.ExitError
				if errors.As(err, &exitError) {
					panic("Error from ffmpeg: " + string(exitError.Stderr))
				}
			}

			producer.Publish(video, ComposerTopic)
			producer.Publish([]byte(fmt.Sprint(time.Now().UnixMilli())), StreamerTopic)
			fmt.Println("video ", files.Video[len(files.Video)-17:len(files.Video)-4], "sent at ", time.Now().UnixMilli())

			if err := os.Remove(files.Video); err != nil {
				panic("Error on deleting video file " + err.Error())
			}
			if err := os.Remove(files.Audio); err != nil {
				panic("Error on deleting audio file " + err.Error())
			}
		case <-quit:
			return
		}
	}
}
