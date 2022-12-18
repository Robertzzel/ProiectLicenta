package main

import (
	"Licenta/Kafka"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	VideoDuration   = time.Second
	VideoTopic      = "video"
	VideoStartTopic = "svideo"
)

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

func stringToTimestamp(s string) (time.Time, error) {
	timestamp, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(timestamp, 0), nil
}

func main() {
	errGroup, ctx := errgroup.WithContext(NewCtx())

	if err := Kafka.CreateTopic(VideoTopic); err != nil {
		panic(err)
	}
	defer Kafka.DeleteTopic(VideoTopic)
	if err := Kafka.CreateTopic(VideoStartTopic); err != nil {
		panic(err)
	}
	defer Kafka.DeleteTopic(VideoStartTopic)

	producer := Kafka.NewProducer()
	defer producer.Close()
	startConsumer := Kafka.NewConsumer(VideoStartTopic)
	defer startConsumer.Close()

	// wait for the start message
	fmt.Println("waiting for message")

	var tsMessage *Kafka.ConsumerMessage
	var err error
	for ctx.Err() == nil {
		tsMessage, err = startConsumer.Consume(time.Second * 2)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			panic(err)
		}

		break
	}
	fmt.Println("Starting..")

	timestamp, err := stringToTimestamp(string(tsMessage.Message))
	if err != nil {
		panic("Timestamp not valid")
	}

	videoRecorder, err := NewRecorder(ctx, 20)
	if err != nil {
		log.Fatal("Recorder cannot be initiated: ", err)
	}

	videoRecorder.Start(timestamp, VideoDuration)

	errGroup.Go(func() error {
		for {
			select {
			case videoName := <-videoRecorder.VideoBuffer:
				if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(videoName), Topic: VideoTopic}); err != nil {
					panic(err)
				}
				log.Println(videoName, time.Now().UnixMilli())
			case <-ctx.Done():
				return nil
			}
		}
	})

	if err := errGroup.Wait(); err != nil {
		log.Println(err)
	}

	defer fmt.Println("Cleanup Done")
}
