package main

import (
	"Licenta/Kafka"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
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
	MergeTopic    = "MERGER"
)

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

func NewCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		cancel()
	}()

	return ctx
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
	//if err := producer.Publish(&Kafka.ProducerMessage{Message: video, Topic: MergeTopic}); err != nil {
	//	return err
	//}

	return nil
}

func GetNextSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer) (AudioVideoPair, error) {
	files := AudioVideoPair{}

	println("START")
	videoMessage, err := videoConsumer.Consume()
	if err != nil {
		print(err)
		return AudioVideoPair{}, err
	}
	println("#")
	audioMessage, err := audioConsumer.Consume()
	if err != nil {
		print(err)
		return AudioVideoPair{}, err
	}

	files.Video = string(videoMessage.Message)
	files.Audio = string(audioMessage.Message)
	println("")
	println("")
	//println(files.Video, " # ", files.Audio)

	videoTimestamp, err := GetFileTimestamp(files.Video)
	if err != nil {
		return AudioVideoPair{}, err
	}

	audioTimestamp, err := GetFileTimestamp(files.Audio)
	if err != nil {
		return AudioVideoPair{}, err
	}

	for videoTimestamp > audioTimestamp {
		audioMessage, err = audioConsumer.Consume(time.Second * 2)
		if err != nil {
			return AudioVideoPair{}, err
		}

		audioTimestamp, err = GetFileTimestamp(string(audioMessage.Message))
		if err != nil {
			return AudioVideoPair{}, err
		}
	}
	files.Audio = string(audioMessage.Message)

	for videoTimestamp < audioTimestamp {
		videoMessage, err = videoConsumer.Consume(time.Second * 2)
		if err != nil {
			return AudioVideoPair{}, err
		}

		videoTimestamp, err = GetFileTimestamp(string(videoMessage.Message))
		if err != nil {
			return AudioVideoPair{}, err
		}
	}
	files.Video = string(videoMessage.Message)

	return files, nil
}

func CollectAudioAndVideoFiles(ctx context.Context, videoConsumer, audioConsumer *Kafka.Consumer, outputChannel chan AudioVideoPair) error {
	for ctx.Err() == nil {
		files, err := GetNextSyncedAudioAndVideo(videoConsumer, audioConsumer)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return err
		}

		outputChannel <- files
	}

	return nil
}

func CompressAndSendFiles(producer *Kafka.Producer, files AudioVideoPair) error {
	defer files.Delete()
	//s := time.Now()

	video, err := CombineAndCompressFiles(files, "1M", "pipe:1")
	if err != nil {
		return err
	}

	if err = SendVideo(producer, video); err != nil {
		return err
	}

	//fmt.Println("video ", files.Video[len(files.Video)-17:len(files.Video)-4], "sent at ", time.Now().UnixMilli(), " ( ", time.Since(s), " ) ", len(video))
	return nil
}

func main() {
	// initialize
	if err := Kafka.CreateTopic(ComposerTopic, 2); err != nil {
		panic(err)
	}

	uiConsumer, err := Kafka.NewConsumer(ComposerTopic, 1)
	if err != nil {
		panic(err)
	}
	defer uiConsumer.Close()

	producer, err := Kafka.NewProducer()
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	filesChannel := make(chan AudioVideoPair, 5)
	errG, ctx := errgroup.WithContext(NewCtx())

	// wait for start
	fmt.Println("WAting for signal")
	for ctx.Err() == nil {
		_, err := uiConsumer.Consume(time.Second * 2)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			panic(err)
		}

		break
	}
	fmt.Println("Starting...")
	ts := fmt.Sprint(time.Now().UnixMilli()/1000 + 1)
	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(ts), Topic: AudioTopic, Partition: 1}); err != nil {
		panic(err)
	}
	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(ts), Topic: VideoTopic, Partition: 1}); err != nil {
		panic(err)
	}

	videoConsumer, err := Kafka.NewConsumer(VideoTopic, 0)
	if err != nil {
		panic(err)
	}
	defer videoConsumer.Close()

	audioConsumer, err := Kafka.NewConsumer(AudioTopic, 0)
	if err != nil {
		panic(err)
	}
	defer audioConsumer.Close()

	// start
	errG.Go(func() error { return CollectAudioAndVideoFiles(ctx, videoConsumer, audioConsumer, filesChannel) })
	errG.Go(func() error {
		for {
			select {
			case filesPair := <-filesChannel:
				errG.Go(func() error { return CompressAndSendFiles(producer, filesPair) })
			case <-ctx.Done():
				return nil
			}
		}
	})

	if err := errG.Wait(); err != nil {
		log.Println(err)
	}

	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte("quit"), Topic: MergeTopic}); err != nil {
		log.Println(err)
	}

	defer fmt.Println("Cleanup Done")
}
