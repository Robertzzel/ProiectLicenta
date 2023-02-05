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

func NewContextCancelableBySignals() context.Context {
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
	println("!!!!!!! ", file, " !!!!!!!")
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

func GetNextAudioAndVideo(ctx context.Context, consumer *Kafka.AggregatorMicroserviceConsumer) (AudioVideoPair, error) {
	files := AudioVideoPair{}
	files.Audio = ""
	files.Video = ""

	for (files.Audio == "" || files.Video == "") && ctx.Err() == nil {
		msg, msgType, err := consumer.ConsumeAggregator(ctx)
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

func UpdateNextVideo(ctx context.Context, consumer *Kafka.AggregatorMicroserviceConsumer, files *AudioVideoPair) error {
	for {
		msg, msgType, err := consumer.ConsumeAggregator(ctx)
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

func UpdateNextAudio(ctx context.Context, consumer *Kafka.AggregatorMicroserviceConsumer, files *AudioVideoPair) error {
	for {
		msg, msgType, err := consumer.ConsumeAggregator(ctx)
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

func GetNextSyncedAudioAndVideo(ctx context.Context, consumer *Kafka.AggregatorMicroserviceConsumer) (AudioVideoPair, error) {
	files, err := GetNextAudioAndVideo(ctx, consumer)
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

		if err = UpdateNextAudio(ctx, consumer, &files); err != nil {
			return AudioVideoPair{}, err
		}

		audioTimestamp, err = GetFileTimestamp(files.Audio)
		if err != nil {
			return AudioVideoPair{}, err
		}
	}

	for videoTimestamp < audioTimestamp {
		if err = UpdateNextVideo(ctx, consumer, &files); err != nil {
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
		files, err := GetNextSyncedAudioAndVideo(ctx, consumer)
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
	defer func(files *AudioVideoPair) {
		_ = files.Delete()
	}(&files)
	// s := time.Now()

	video, err := CombineAndCompressFiles(files, "1M", "pipe:1")
	if err != nil {
		return err
	}

	if err = SendVideo(producer, video); err != nil {
		return err
	}

	// fmt.Println("video ", files.Video[len(files.Video)-17:len(files.Video)-4], "sent at ", time.Now().UnixMilli(), " ( ", time.Since(s), " ) ", len(video))
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
	return ctx.Err()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("No broker address and topics given")
		return
	}
	brokerAddress := os.Args[1]
	topic := os.Args[2]

	producer, err := Kafka.NewAggregatorMicroserviceProducer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}
	consumer, err := Kafka.NewAggregatorMicroserviceConsumer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	errorGroup, ctx := errgroup.WithContext(NewContextCancelableBySignals())
	if err = waitForAUser(ctx, consumer); err != nil {
		panic(err)
	}

	ts := fmt.Sprint(time.Now().UnixMilli()/1000 + 1)
	if err = producer.PublishAudioStart([]byte(ts), nil); err != nil {
		panic(err)
	}
	if err = producer.PublishVideoStart([]byte(ts), nil); err != nil {
		panic(err)
	}
	producer.Flush(500)

	filesChannel := make(chan AudioVideoPair, 5)
	errorGroup.Go(func() error { return CollectAudioAndVideoFiles(ctx, consumer, filesChannel) })
	errorGroup.Go(func() error {
		for {
			select {
			case filesPair := <-filesChannel:
				errorGroup.Go(func() error { return CompressAndSendFiles(producer, filesPair) })
			case <-ctx.Done():
				return nil
			}
		}
	})

	if err = errorGroup.Wait(); err != nil {
		log.Println(err)
	}

	if err = producer.PublishMerger([]byte("quit"), nil); err != nil {
		log.Println(err)
	}
	producer.Flush(1000)
	producer.Close()

	defer log.Println("Cleanup Done")
}
