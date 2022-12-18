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
	"strings"
	"syscall"
	"time"
)

const (
	VideoTopic           = "video"
	VideoStartTopic      = "svideo"
	AudioTopic           = "audio"
	AudioStartTopic      = "saudio"
	AggregatorTopic      = "aggregator"
	AggregatorStartTopic = "saggregator"
	MergeTopic           = "MERGER"
)

type AudioVideoPair struct {
	Video string
	Audio string
}

func Map[A any, B any](input []A, f func(A) B) []B {
	result := make([]B, len(input))
	for i, value := range input {
		result[i] = f(value)
	}
	return result
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

func SendVideo(producer *Kafka.Producer, video []byte, users []int) error {
	if err := producer.Publish(&Kafka.ProducerMessage{Message: video, Topic: AggregatorTopic}); err != nil {
		return err
	}

	parsedUsers := Map(users, func(a int) string { return strconv.Itoa(a) })
	if err := producer.Publish(&Kafka.ProducerMessage{Message: video, Topic: MergeTopic, Headers: []Kafka.Header{
		{"users", []byte(strings.Join(parsedUsers, ","))}}}); err != nil {
		return err
	}

	return nil
}

func GetNextSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer) (AudioVideoPair, error) {
	files := AudioVideoPair{}

	videoMessage, err := videoConsumer.Consume()
	if err != nil {
		print(err)
		return AudioVideoPair{}, err
	}
	audioMessage, err := audioConsumer.Consume()
	if err != nil {
		print(err)
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

func CompressAndSendFiles(producer *Kafka.Producer, files AudioVideoPair, users []int) error {
	defer files.Delete()
	s := time.Now()

	video, err := CombineAndCompressFiles(files, "1M", "pipe:1")
	if err != nil {
		return err
	}

	if err = SendVideo(producer, video, users); err != nil {
		return err
	}

	fmt.Println("video ", files.Video[len(files.Video)-17:len(files.Video)-4], "sent at ", time.Now().UnixMilli(), " ( ", time.Since(s), " ) ", len(video))
	return nil
}

func captureConnectedUsers(ctx context.Context, users *[]int) error {
	if err := Kafka.CreateTopic(AggregatorStartTopic); err != nil {
		return err
	}

	uiConsumer := Kafka.NewConsumer(AggregatorStartTopic)
	defer uiConsumer.Close()

	if err := uiConsumer.SetOffsetToNow(); err != nil {
		return err
	}

	var newUserMessage *Kafka.ConsumerMessage
	var err error
	for ctx.Err() == nil {
		newUserMessage, err = uiConsumer.Consume(time.Second * 2)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return err
		}

		userId, err := strconv.Atoi(string(newUserMessage.Message))
		if err != nil {
			return err
		}

		*users = append(*users, userId)
	}

	return nil
}

func main() {
	// initialize
	if err := Kafka.CreateTopic(AggregatorTopic); err != nil {
		panic(err)
	}

	producer := Kafka.NewProducer()
	defer producer.Close()

	errG, ctx := errgroup.WithContext(NewCtx())

	// wait for start
	fmt.Println("Waiting for signal")
	var usersConnected []int
	errG.Go(func() error { return captureConnectedUsers(ctx, &usersConnected) })
	for len(usersConnected) == 0 {
		time.Sleep(time.Second / 2)
	}

	fmt.Println("Starting...")
	ts := fmt.Sprint(time.Now().UnixMilli()/1000 + 1)
	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(ts), Topic: AudioStartTopic}); err != nil {
		panic(err)
	}
	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(ts), Topic: VideoStartTopic}); err != nil {
		panic(err)
	}

	videoConsumer := Kafka.NewConsumer(VideoTopic)
	defer videoConsumer.Close()

	audioConsumer := Kafka.NewConsumer(AudioTopic)
	defer audioConsumer.Close()

	// start
	filesChannel := make(chan AudioVideoPair, 5)
	errG.Go(func() error { return CollectAudioAndVideoFiles(ctx, videoConsumer, audioConsumer, filesChannel) })
	errG.Go(func() error {
		for {
			select {
			case filesPair := <-filesChannel:
				fmt.Println(len(usersConnected))
				errG.Go(func() error { return CompressAndSendFiles(producer, filesPair, usersConnected) })
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
