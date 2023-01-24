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
)

var (
	VideoTopic      string
	VideoStartTopic string
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

func getStartTime(ctx context.Context, consumer *Kafka.Consumer) (string, error) {
	var tsMessage *Kafka.ConsumerMessage
	var err error

	for ctx.Err() == nil {
		tsMessage, err = consumer.Consume(time.Second / 4)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return "", err
		}

		return string(tsMessage.Message), nil
	}

	return "", ctx.Err()
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("No broker address given")
		return
	}
	brokerAddress := os.Args[1]
	VideoTopic = os.Args[2]
	VideoStartTopic = "s" + VideoTopic

	errGroup, ctx := errgroup.WithContext(NewCtx())

	producer := Kafka.NewProducer(brokerAddress)

	startConsumer := Kafka.NewConsumer(brokerAddress, VideoStartTopic)
	if err := startConsumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	startTimestamp, err := getStartTime(ctx, startConsumer)
	if err != nil {
		panic(err)
	}

	timestamp, err := stringToTimestamp(startTimestamp)
	if err != nil {
		panic(err)
	}

	videoRecorder, err := NewRecorder(ctx, 15)
	if err != nil {
		log.Fatal("Recorder cannot be initiated: ", err)
	}

	videoRecorder.Start(timestamp, VideoDuration)

	errGroup.Go(func() error {
		for ctx.Err() == nil {
			if err = producer.Publish(&Kafka.ProducerMessage{Message: []byte(<-videoRecorder.VideoBuffer), Topic: VideoTopic}); err != nil {
				return err
			}
		}
		return nil
	})

	if err = errGroup.Wait(); err != nil {
		log.Println(err)
	}

	defer fmt.Println("Cleanup Done")
}
