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
	VideoDuration = time.Second
	VideoTopic    = "video"
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

	if err := Kafka.CreateTopic(VideoTopic, 2); err != nil {
		panic(err)
	}
	defer Kafka.DeleteTopic(VideoTopic)

	producer := Kafka.NewProducer()
	defer producer.Close()
	consumer := Kafka.NewConsumer(VideoTopic, 1)

	// wait for the start message
	fmt.Println("waiting for message")
	var tsMessage *Kafka.ConsumerMessage
	var err error
	for ctx.Err() == nil {
		tsMessage, err = consumer.Consume(time.Second * 2)
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("empty")
			continue
		} else if err != nil {
			panic(err)
		}

		break
	}
	timestamp, err := stringToTimestamp(string(tsMessage.Message))
	if err != nil {
		panic("Timestamp not valid")
	}
	fmt.Println("Starting..")

	videoRecorder, err := NewRecorder(ctx, 20)
	if err != nil {
		log.Fatal("Recorder cannot be initiated: ", err)
	}

	videoRecorder.Start(timestamp, VideoDuration)

	errGroup.Go(func() error {
		for {
			select {
			case videoName := <-videoRecorder.VideoBuffer:
				if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(videoName), Topic: VideoTopic, Partition: 0}); err != nil {
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
