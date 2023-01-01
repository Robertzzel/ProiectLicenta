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

func main() {
	if len(os.Args) < 3 {
		fmt.Println("No broker address given")
		return
	}
	brokerAddress := os.Args[1]
	VideoTopic = os.Args[2]
	VideoStartTopic = "s" + VideoTopic

	errGroup, ctx := errgroup.WithContext(NewCtx())

	if err := Kafka.CreateTopic(brokerAddress, VideoTopic); err != nil {
		panic(err)
	}
	defer func(brokerAddress string, topic string) {
		err := Kafka.DeleteTopic(brokerAddress, topic)
		if err != nil {
			fmt.Println(err)
		}
	}(brokerAddress, VideoTopic)

	if err := Kafka.CreateTopic(brokerAddress, VideoStartTopic); err != nil {
		panic(err)
	}
	defer func(brokerAddress string, names string) {
		err := Kafka.DeleteTopic(brokerAddress, names)
		if err != nil {
			fmt.Println(err)
		}
	}(brokerAddress, VideoStartTopic)

	producer := Kafka.NewProducer(brokerAddress)
	defer producer.Close()

	startConsumer := Kafka.NewConsumer(brokerAddress, VideoStartTopic)
	defer startConsumer.Close()
	if err := startConsumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	// wait for the start message
	fmt.Println("waiting for message")

	var tsMessage *Kafka.ConsumerMessage
	var err error
	for ctx.Err() == nil {
		tsMessage, err = startConsumer.Consume(time.Second / 2)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			panic(err)
		}

		break
	}
	if ctx.Err() != nil {
		fmt.Println("Video: ", ctx.Err())
		return
	}
	fmt.Println("Starting..")

	timestamp, err := stringToTimestamp(string(tsMessage.Message))
	if err != nil {
		panic("Timestamp not valid")
	}

	videoRecorder, err := NewRecorder(ctx, 15)
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
				fmt.Println(videoName, time.Now().UnixMilli())
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
