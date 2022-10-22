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

func getSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer, outputChannel chan AudioVideoPair) {
	for {
		audioMessage, err := audioConsumer.Consume()
		if err != nil {
			log.Println("Error while receiving audio file: ", err)
			continue
		}

		videoMessage, err := videoConsumer.Consume()
		if err != nil {
			log.Println("Error while receiving video file: ", err)
			continue
		}

		videoFile := string(videoMessage.Value)
		audioFile := string(audioMessage.Value)

		videoTimestamp, err := strconv.Atoi(videoFile[len(videoFile)-14 : len(videoFile)-4])
		if err != nil {
			log.Println("Error, video timestamp not valid")
			continue
		}
		audioTimestamp, err := strconv.Atoi(audioFile[len(audioFile)-14 : len(audioFile)-4])
		if err != nil {
			log.Println("Error, audio timestamp not valid")
			continue
		}

		timestampDifference := videoTimestamp - audioTimestamp
		if timestampDifference > 0 {
			log.Println("Desync ", "audio by ", timestampDifference)
			for i := 0; i < timestampDifference; i++ {
				audioMessage, err = audioConsumer.Consume()
				if err != nil {
					log.Println("Error while receiving audio file: ", err)
					continue
				}
			}
			audioFile = string(audioMessage.Value)
		} else if timestampDifference < 0 {
			log.Println("Desync ", "video by ", timestampDifference)
			for i := 0; i < -timestampDifference; i++ {
				videoMessage, err = videoConsumer.Consume()
				if err != nil {
					log.Println("Error while receiving video file: ", err)
					continue
				}
			}
			videoFile = string(videoMessage.Value)
		}

		outputChannel <- AudioVideoPair{Video: videoFile, Audio: audioFile}
	}
}

func main() {
	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	if err := Kafka.CreateTopic(ComposerTopic); err != nil {
		log.Fatal("Cannot create aggregator topic ", err)
	}

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
			video, err := exec.Command("./CombineAndCompress", files.Video, files.Audio, "6m").Output()
			if err != nil {
				var exitError *exec.ExitError
				if errors.As(err, &exitError) {
					panic("Error from ffmpeg: " + string(exitError.Stderr))
				}
			}

			fmt.Println("Sending video of size ", len(video))
			if err := composerProducer.Publish(video); err != nil {
				panic("Error on video sending: " + err.Error())
			}
			fmt.Println("video ", files.Video[len(files.Video)-14:len(files.Video)-4], "sent at ", time.Now().UnixMilli())

			if err := streamerProducer.Publish([]byte(fmt.Sprint(time.Now().UnixMilli()))); err != nil {
				log.Println("Error while sending ping signal ", err)
			}

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
