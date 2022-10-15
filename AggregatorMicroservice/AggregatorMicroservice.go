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

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func getSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer) (string, string, error) {
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

	return videoFile, audioFile, nil
}

func main() {
	checkErr(Kafka.CreateTopic(ComposerTopic))

	videoConsumer := Kafka.NewConsumer(VideoTopic)
	audioConsumer := Kafka.NewConsumer(AudioTopic)
	composerProducer := Kafka.NewProducerAsync(ComposerTopic)
	streamerProducer := Kafka.NewProducerAsync(StreamerTopic)

	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		Kafka.DeleteTopic(ComposerTopic)
		fmt.Println("Cleanup done")
		os.Exit(1)
	}()

	checkErr(videoConsumer.SetOffsetToNow())
	checkErr(audioConsumer.SetOffsetToNow())

	for {
		videoFile, audioFile, err := getSyncedAudioAndVideo(videoConsumer, audioConsumer)
		checkErr(err)

		go func(videoFile, audioFile string) {
			s := time.Now()

			video, err := exec.Command("./CombineAndCompress", videoFile, audioFile, "1023k").Output()
			checkErr(err)

			checkErr(composerProducer.Publish(video))

			checkErr(streamerProducer.Publish([]byte(fmt.Sprint(time.Now().UnixMilli()))))
			fmt.Println("timestamp: ", videoFile[len(videoFile)-17:len(videoFile)-4], " at", time.Now().UnixMilli(), " (", time.Since(s), " )")

			checkErr(os.Remove(videoFile))
			checkErr(os.Remove(audioFile))
		}(videoFile, audioFile)
	}
}
