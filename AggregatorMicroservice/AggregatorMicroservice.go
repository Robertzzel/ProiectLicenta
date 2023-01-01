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

var (
	AggregatorTopic      string
	AggregatorStartTopic string
	VideoTopic           string
	VideoStartTopic      string
	AudioTopic           string
	AudioStartTopic      string
	MergeTopic           string
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
	if err := producer.Publish(&Kafka.ProducerMessage{Message: video, Topic: AggregatorTopic}); err != nil {
		return err
	}

	if err := producer.Publish(&Kafka.ProducerMessage{Message: video, Topic: MergeTopic}); err != nil {
		return err
	}

	return nil
}

func GetNextSyncedAudioAndVideo(videoConsumer, audioConsumer *Kafka.Consumer) (AudioVideoPair, error) {
	files := AudioVideoPair{}

	videoMessage, err := videoConsumer.Consume(time.Second)
	if err != nil {
		return AudioVideoPair{}, err
	}
	audioMessage, err := audioConsumer.Consume(time.Second)
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
	s := time.Now()

	video, err := CombineAndCompressFiles(files, "1M", "pipe:1")
	if err != nil {
		return err
	}

	if err = SendVideo(producer, video); err != nil {
		return err
	}

	fmt.Println("video ", files.Video[len(files.Video)-17:len(files.Video)-4], "sent at ", time.Now().UnixMilli(), " ( ", time.Since(s), " ) ", len(video))
	return nil
}

func waitForAUser(ctx context.Context, brokerAddress string) error {
	uiConsumer := Kafka.NewConsumer(brokerAddress, AggregatorStartTopic)
	defer uiConsumer.Close()

	if err := uiConsumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	for ctx.Err() == nil {
		_, err := uiConsumer.Consume(time.Second * 2)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func main() {
	if len(os.Args) < 6 {
		fmt.Println("No broker address and topics given")
		return
	}
	brokerAddress := os.Args[1]
	AggregatorTopic = os.Args[2]
	AggregatorStartTopic = "s" + AggregatorTopic
	VideoTopic = os.Args[3]
	VideoStartTopic = "s" + VideoTopic
	AudioTopic = os.Args[4]
	AudioStartTopic = "s" + AudioTopic
	MergeTopic = os.Args[5]
	log.Println("Aggregator: ", MergeTopic)

	// initialize
	if err := Kafka.CreateTopic(brokerAddress, AggregatorTopic); err != nil {
		panic(err)
	}

	producer := Kafka.NewProducer(brokerAddress)
	defer producer.Close()

	errG, ctx := errgroup.WithContext(NewCtx())

	if err := Kafka.CreateTopic(brokerAddress, AggregatorStartTopic); err != nil {
		panic(err)
	}
	defer Kafka.DeleteTopic(brokerAddress, AggregatorStartTopic)

	fmt.Println("Waiting for a signal")
	if err := waitForAUser(ctx, brokerAddress); err != nil {
		panic(err)
	}
	fmt.Println("Starting aggregator...")

	ts := fmt.Sprint(time.Now().UnixMilli()/1000 + 1)
	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(ts), Topic: AudioStartTopic}); err != nil {
		panic(err)
	}
	if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(ts), Topic: VideoStartTopic}); err != nil {
		panic(err)
	}

	videoConsumer := Kafka.NewConsumer(brokerAddress, VideoTopic)
	defer videoConsumer.Close()

	audioConsumer := Kafka.NewConsumer(brokerAddress, AudioTopic)
	defer audioConsumer.Close()

	// start
	filesChannel := make(chan AudioVideoPair, 5)
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

	fmt.Println("Quit signal sent")

	defer fmt.Println("Cleanup Done")
}
