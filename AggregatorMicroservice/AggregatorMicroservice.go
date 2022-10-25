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

var quit = make(chan os.Signal, 2)

type AudioVideoPair struct {
	Video string
	Audio string
}

func getSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer, outputChannel chan AudioVideoPair) {
	for {
		var videoMessage, audioMessage []byte
		var err error

		videoMessage, err = videoConsumer.Consume()
		if err != nil {
			log.Println(err)
			quit <- syscall.SIGINT
			return
		}

		audioMessage, err = audioConsumer.Consume()
		if err != nil {
			log.Println(err)
			quit <- syscall.SIGINT
			return
		}

		videoFile := string(videoMessage)
		audioFile := string(audioMessage)

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
					log.Println(err)
					quit <- syscall.SIGINT
					return
				}
			}
			audioFile = string(audioMessage)
		} else if timestampDifference < 0 {
			log.Println("Desync ", "video by ", timestampDifference)
			for i := 0; i < -timestampDifference; i++ {
				videoMessage, err = videoConsumer.Consume()
				if err != nil {
					log.Println(err)
					quit <- syscall.SIGINT
					return
				}
			}
			videoFile = string(videoMessage)
		}

		outputChannel <- AudioVideoPair{Video: videoFile, Audio: audioFile}
	}
}

func main() {
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	videoConsumer, err := Kafka.NewConsumer(VideoTopic)
	if err != nil {
		panic(err)
	}
	defer func() {
		videoConsumer.Close()
		log.Println("video consumer closed")
	}()

	audioConsumer, err := Kafka.NewConsumer(AudioTopic)
	if err != nil {
		panic(err)
	}
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
			video, err := exec.Command("./CombineAndCompress", files.Video, files.Audio, "1023k").Output()
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
