package main

import (
	"Licenta/Kafka"
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

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func getSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer, outputChannel chan AudioVideoPair) {
	for {
		videoMessage, err := videoConsumer.Consume()
		checkErr(err)
		audioMessage, err := audioConsumer.Consume()
		checkErr(err)

		videoFile := string(videoMessage.Value)
		audioFile := string(audioMessage.Value)

		videoTimestamp, err := strconv.Atoi(videoFile[len(videoFile)-14 : len(videoFile)-4])
		checkErr(err)
		audioTimestamp, err := strconv.Atoi(audioFile[len(audioFile)-14 : len(audioFile)-4])
		checkErr(err)

		timestampDifference := videoTimestamp - audioTimestamp
		if timestampDifference > 0 {
			log.Println("Desync ", "audio by ", timestampDifference)
			for i := 0; i < timestampDifference; i++ {
				audioMessage, err = audioConsumer.Consume()
				checkErr(err)
			}
			audioFile = string(audioMessage.Value)
		} else if timestampDifference < 0 {
			log.Println("Desync ", "video by ", timestampDifference)
			for i := 0; i < -timestampDifference; i++ {
				videoMessage, err = videoConsumer.Consume()
				checkErr(err)
			}
			videoFile = string(videoMessage.Value)
		}

		outputChannel <- AudioVideoPair{Video: videoFile, Audio: audioFile}
	}
}

func main() {
	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	checkErr(Kafka.CreateTopic(ComposerTopic))
	videoConsumer := Kafka.NewConsumer(VideoTopic)
	audioConsumer := Kafka.NewConsumer(AudioTopic)
	composerProducer := Kafka.NewProducerAsync(ComposerTopic)
	streamerProducer := Kafka.NewProducerAsync(StreamerTopic)
	defer func() {
		fmt.Println("Starting cleaning")
		Kafka.DeleteTopic(ComposerTopic)
		videoConsumer.Close()
		audioConsumer.Close()
		composerProducer.Close()
		streamerProducer.Close()
		fmt.Println("Cleaning done")
	}()

	filesChannel := make(chan AudioVideoPair, 5)
	go getSyncedAudioAndVideo(videoConsumer, audioConsumer, filesChannel)

	for {
		select {
		case files := <-filesChannel:
			s := time.Now()

			video, err := exec.Command("./CombineAndCompress", files.Video, files.Audio, "1023k").Output()
			checkErr(err)

			checkErr(composerProducer.Publish(video))
			fmt.Println("Extrast")

			checkErr(streamerProducer.Publish([]byte(fmt.Sprint(time.Now().UnixMilli()))))
			fmt.Println("timestamp: ", files.Video[len(files.Video)-14:len(files.Video)-4], " at", time.Now().UnixMilli(), " (", time.Since(s), " )")

			checkErr(os.Remove(files.Video))
			checkErr(os.Remove(files.Audio))
		case <-quit:
			return
		}
	}
}
