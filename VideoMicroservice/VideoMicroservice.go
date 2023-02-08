package main

import (
	"Licenta/Kafka"
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

func getStartTime(ctx context.Context, brokerAddress, topic string) (string, error) {
	consumer, err := Kafka.NewVideoMicroserviceConsumer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	for ctx.Err() == nil {
		tsMessage, _, err := consumer.Consume(time.Second / 5)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return "", err
		}

		return string(tsMessage), nil
	}

	_ = consumer.Close()
	return "", ctx.Err()
}

func main() {
	if len(os.Args) < 2 {
		log.Println("No broker address given")
		return
	}
	brokerAddress := os.Args[1]
	topic := os.Args[2]

	errGroup, ctx := errgroup.WithContext(NewCtx())

	producer, err := Kafka.NewVideoMicroserviceProducer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	startTimestamp, err := getStartTime(ctx, brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	timestamp, err := stringToTimestamp(startTimestamp)
	if err != nil {
		panic(err)
	}

	videoRecorder, err := NewRecorder(ctx, 20)
	if err != nil {
		panic(err)
	}

	videoRecorder.Start(timestamp, VideoDuration)

	errGroup.Go(func() error {
		for ctx.Err() == nil {
			if err = producer.Publish([]byte(<-videoRecorder.VideoBuffer), []kafka.Header{{"type", []byte("video")}}); err != nil {
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
