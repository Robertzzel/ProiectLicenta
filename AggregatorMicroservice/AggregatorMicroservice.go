package main

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"licenta/Kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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

	videoTimestamp, err := files.GetVideoTimestamp()
	if err != nil {
		return AudioVideoPair{}, err
	}

	audioTimestamp, err := files.GetAudioTimestamp()
	if err != nil {
		return AudioVideoPair{}, err
	}

	for videoTimestamp != audioTimestamp {
		if videoTimestamp > audioTimestamp {
			if err = UpdateNextAudio(ctx, consumer, &files); err != nil {
				return AudioVideoPair{}, err
			}
			audioTimestamp, err = files.GetAudioTimestamp()
			if err != nil {
				return AudioVideoPair{}, err
			}
		} else {
			if err = UpdateNextVideo(ctx, consumer, &files); err != nil {
				return AudioVideoPair{}, err
			}
			videoTimestamp, err = files.GetAudioTimestamp()
			if err != nil {
				return AudioVideoPair{}, err
			}
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

	video, err := files.CombineAndCompress("5000k", "pipe:1")
	if err != nil {
		return err
	}

	if err = producer.PublishClient(video, nil); err != nil {
		return err
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

	producer, err := Kafka.NewAggregatorMicroserviceProducer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}
	consumer, err := Kafka.NewAggregatorMicroserviceConsumer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	errorGroup, ctx := errgroup.WithContext(NewContextCancelableBySignals())
	time.Sleep(time.Second * 2)

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
				fmt.Println("AGG BEFORE COMPRESSING")
				err := CompressAndSendFiles(producer, filesPair)
				if err != nil {
					fmt.Println("Aggregaot sending file error", err)
					return err
				}
				fmt.Println("Aggregator sent file")
			case <-ctx.Done():
				return nil
			}
		}
	})

	if err = errorGroup.Wait(); err != nil {
		log.Println(err)
	}

	if err = producer.PublishClient([]byte("quit"), nil); err != nil {
		log.Println(err)
	}
	producer.Flush(1000)
	producer.Close()

	defer log.Println("Cleanup Done")
}
