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

func SendVideo(producer *Kafka.AggregatorMicroserviceProducer, video []byte) error {
	if err := producer.PublishClient(video, nil); err != nil {
		return err
	}

	if err := producer.PublishMerger(video, nil); err != nil {
		return err
	}

	return nil
}

func GetNextAudioAndVideo(consumer *Kafka.AggregatorMicroserviceConsumer, timeout time.Duration) (AudioVideoPair, error) {
	files := AudioVideoPair{}
	files.Audio = ""
	files.Video = ""

	for files.Audio == "" || files.Video == "" {
		msg, msgType, err := consumer.ConsumeAggregator()
		if err != nil {
			return AudioVideoPair{}, err
		}

		if msgType == Kafka.VideoMessage {
			files.Video = string(msg)
		} else if msgType == Kafka.AudioMessage {
			files.Audio = string(msg)
		}
	}

	return files, nil
}

func UpdateNextVideo(consumer *Kafka.AggregatorMicroserviceConsumer, files *AudioVideoPair, timeout time.Duration) error {
	for {
		msg, msgType, err := consumer.ConsumeAggregator(timeout)
		if err != nil {
			return err
		}

		if msgType == Kafka.VideoMessage {
			files.Video = string(msg)
			break
		} else if msgType == Kafka.AudioMessage {
			files.Audio = string(msg)
		}
	}

	return nil
}

func UpdateNextAudio(consumer *Kafka.AggregatorMicroserviceConsumer, files *AudioVideoPair, timeout time.Duration) error {
	for {
		msg, msgType, err := consumer.ConsumeAggregator(timeout)
		if err != nil {
			return err
		}

		if msgType == Kafka.VideoMessage {
			files.Video = string(msg)
		} else if msgType == Kafka.AudioMessage {
			files.Audio = string(msg)
			break
		}
	}

	return nil
}

func GetNextSyncedAudioAndVideo(consumer *Kafka.AggregatorMicroserviceConsumer) (AudioVideoPair, error) {
	files, err := GetNextAudioAndVideo(consumer, time.Second)
	if err != nil {
		return AudioVideoPair{}, err
	}

	videoTimestamp, err := GetFileTimestamp(files.Video)
	if err != nil {
		return AudioVideoPair{}, err
	}

	audioTimestamp, err := GetFileTimestamp(files.Audio)
	if err != nil {
		return AudioVideoPair{}, err
	}

	for videoTimestamp > audioTimestamp {

		if err = UpdateNextAudio(consumer, &files, time.Second); err != nil {
			return AudioVideoPair{}, err
		}

		audioTimestamp, err = GetFileTimestamp(files.Audio)
		if err != nil {
			return AudioVideoPair{}, err
		}
	}

	for videoTimestamp < audioTimestamp {
		if err = UpdateNextVideo(consumer, &files, time.Second); err != nil {
			return AudioVideoPair{}, err
		}

		videoTimestamp, err = GetFileTimestamp(files.Video)
		if err != nil {
			return AudioVideoPair{}, err
		}
	}

	return files, nil
}

func CollectAudioAndVideoFiles(ctx context.Context, consumer *Kafka.AggregatorMicroserviceConsumer, outputChannel chan AudioVideoPair) error {
	for ctx.Err() == nil {
		files, err := GetNextSyncedAudioAndVideo(consumer)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return err
		}

		outputChannel <- files
	}

	return nil
}

func CompressAndSendFiles(producer *Kafka.AggregatorMicroserviceProducer, files AudioVideoPair) error {
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

func waitForAUser(ctx context.Context, consumer *Kafka.AggregatorMicroserviceConsumer) error {

	for ctx.Err() == nil {
		_, _, err := consumer.ConsumeAggregatorStart(time.Second * 2)
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
	if len(os.Args) < 2 {
		fmt.Println("No broker address and topics given")
		return
	}
	brokerAddress := os.Args[1]
	topic := os.Args[2]

	// initialize

	producer, err := Kafka.NewAggregatorMicroserviceProducer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	errG, ctx := errgroup.WithContext(NewCtx())

	fmt.Println("Waiting for a signal")
	consumer, err := Kafka.NewAggregatorMicroserviceConsumer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	if err := waitForAUser(ctx, consumer); err != nil {
		panic(err)
	}
	fmt.Println("Starting aggregator...")

	ts := fmt.Sprint(time.Now().UnixMilli()/1000 + 1)
	if err = producer.PublishAudioStart([]byte(ts), nil); err != nil {
		panic(err)
	}
	if err = producer.PublishVideoStart([]byte(ts), nil); err != nil {
		panic(err)
	}

	producer.Flush(500)
	// start
	filesChannel := make(chan AudioVideoPair, 5)
	errG.Go(func() error { return CollectAudioAndVideoFiles(ctx, consumer, filesChannel) })
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

	if err = errG.Wait(); err != nil {
		log.Println(err)
	}

	if err := producer.PublishMerger([]byte("quit"), nil); err != nil {
		log.Println(err)
	}
	producer.Flush(1000)
	producer.Close()

	fmt.Println("Quit signal sent")

	defer fmt.Println("Cleanup Done")
}
