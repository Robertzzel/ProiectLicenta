package main

import (
	"context"
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
	VideoDuration = time.Second / 5
)

func NewCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		cancel()
		fmt.Println("context cancelled")
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

func getStartTime(ctx context.Context, conn KafkaConnection) (string, error) {
	for ctx.Err() == nil {
		msg, err := conn.Consume(ctx)
		if err != nil {
			return "", err
		}

		return string(msg.Value), nil
	}
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

	kafkaConnection, err := NewKafkaConnection(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	startTimestamp, err := getStartTime(ctx, kafkaConnection)
	if err != nil {
		panic(err)
	}

	timestamp, err := stringToTimestamp(startTimestamp)
	if err != nil {
		panic(err)
	}

	videoRecorder, err := NewRecorder(ctx, 60)
	if err != nil {
		panic(err)
	}

	videoRecorder.Start(timestamp, VideoDuration)

	errGroup.Go(func() error {
		for ctx.Err() == nil {
			if err = kafkaConnection.Publish([]byte(<-videoRecorder.VideoBuffer), []kafka.Header{{"type", []byte("video")}}); err != nil {
				fmt.Println("Video block err", err)
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
