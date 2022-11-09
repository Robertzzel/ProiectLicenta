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

func (avp *AudioVideoPair) Delete() error {
	if err := os.Remove(avp.Video); err != nil {
		return err
	}

	if err := os.Remove(avp.Audio); err != nil {
		return err
	}

	return nil
}

func GetFileTimestamp(file string) (int, error) {
	return strconv.Atoi(file[len(file)-17 : len(file)-4])
}

func CombineAndCompressFiles(files AudioVideoPair, bitrate string, output string) ([]byte, error) {
	result, err := exec.Command("./CombineAndCompress", files.Video, files.Audio, bitrate, output).Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return nil, errors.New(string(exitError.Stderr))
		}
	}

	return result, nil
}

func SendVideo(producer *Kafka.Producer, video []byte) error {
	if err := producer.Publish(&Kafka.ProducerMessage{Message: video, Topic: ComposerTopic}); err != nil {
		return err
	}

	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(fmt.Sprint(time.Now().UnixMilli())), Topic: StreamerTopic}); err != nil {
		return err
	}

	return nil
}

func GetNextSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer) (AudioVideoPair, error) {
	files := AudioVideoPair{}

	videoMessage, err := videoConsumer.Consume()
	if err != nil {
		return AudioVideoPair{}, err
	}

	audioMessage, err := audioConsumer.Consume()
	if err != nil {
		return AudioVideoPair{}, err
	}

	files.Video = string(videoMessage.Message)
	files.Audio = string(audioMessage.Message)

	videoTimestamp, err := GetFileTimestamp(files.Video)
	if err != nil {
		return AudioVideoPair{}, err
	}

	audioTimestamp, err := GetFileTimestamp(files.Audio)
	if err != nil {
		return AudioVideoPair{}, err
	}

	for videoTimestamp > audioTimestamp {
		audioMessage, err = audioConsumer.Consume()
		if err != nil {
			return AudioVideoPair{}, err
		}

		audioTimestamp, err = GetFileTimestamp(files.Audio)
		if err != nil {
			return AudioVideoPair{}, err
		}
	}
	files.Audio = string(audioMessage.Message)

	for videoTimestamp < audioTimestamp {
		videoMessage, err = videoConsumer.Consume()
		if err != nil {
			return AudioVideoPair{}, err
		}

		videoTimestamp, err = GetFileTimestamp(files.Video)
		if err != nil {
			return AudioVideoPair{}, err
		}
	}
	files.Video = string(videoMessage.Message)

	return files, nil
}

func CollectAudioAndVideoFiles(videoConsumer, audioConsumer *Kafka.Consumer, outputChannel chan AudioVideoPair) {
	for {
		files, err := GetNextSyncedAudioAndVideo(videoConsumer, audioConsumer)
		if err != nil {
			log.Println(err)
			quit <- syscall.SIGINT
			return
		}

		outputChannel <- files
	}
}

func CompressAndSendFiles(producer *Kafka.Producer, files AudioVideoPair) error {
	defer files.Delete()
	s := time.Now()

	video, err := CombineAndCompressFiles(files, "2M", "pipe:1")
	if err != nil {
		return err
	}

	if err = SendVideo(producer, video); err != nil {
		return err
	}
	//if err := os.WriteFile(fmt.Sprintf("./audioVideos/%d.mp4", time.Now().UnixMilli()), video, 0777); err != nil {
	//	return err
	//}

	fmt.Println("video ", files.Video[len(files.Video)-14:len(files.Video)-4], "sent at ", time.Now().UnixMilli(), " ( ", time.Since(s), " ) ", len(video))
	return nil
}

func main() {
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	videoConsumer := Kafka.NewConsumer(VideoTopic)
	defer func() {
		videoConsumer.Close()
		log.Println("video consumer closed")
	}()

	audioConsumer := Kafka.NewConsumer(AudioTopic)
	defer func() {
		audioConsumer.Close()
		log.Println("video consumer closed")
	}()

	if err := Kafka.CreateTopic(ComposerTopic); err != nil {
		panic(err)
	}

	producer := Kafka.NewProducer()
	defer func() {
		producer.Close()
		Kafka.DeleteTopic(ComposerTopic)
		log.Println("producer and topic closed")
	}()

	filesChannel := make(chan AudioVideoPair, 5)
	go CollectAudioAndVideoFiles(videoConsumer, audioConsumer, filesChannel)

	for {
		select {
		case filesPair := <-filesChannel:
			go func(files AudioVideoPair) {
				if err := CompressAndSendFiles(producer, files); err != nil {
					fmt.Println(err)
					quit <- syscall.SIGINT
					return
				}
			}(filesPair)
		case <-quit:
			if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte("quit"), Topic: ComposerTopic}); err != nil {
				log.Println(err)
			}
			fmt.Println("Sent")
			return
		}
	}
}
