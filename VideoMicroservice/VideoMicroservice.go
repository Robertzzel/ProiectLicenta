package main

import (
	"Licenta/Kafka"
	"context"
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
	if len(os.Args) < 2 {
		log.Fatal("No timestamp given")
	}

	timestamp, err := stringToTimestamp(os.Args[1])
	if err != nil {
		log.Fatal("Timestamp not valid")
	}

	errGroup, ctx := errgroup.WithContext(NewCtx())

	videoRecorder, err := NewRecorder(ctx, 20)
	if err != nil {
		log.Fatal("Recorder cannot be initiated: ", err)
	}

	errGroup.Go(func() error { return videoRecorder.Start(timestamp, VideoDuration) })

	if err := Kafka.CreateTopic(VideoTopic); err != nil {
		panic(err)
	}
	defer Kafka.DeleteTopic(VideoTopic)

	composer := Kafka.NewProducer()
	defer composer.Close()

	errGroup.Go(func() error {
		for {
			select {
			case videoName := <-videoRecorder.VideoBuffer:
				if err := composer.Publish(&Kafka.ProducerMessage{Message: []byte(videoName), Topic: VideoTopic}); err != nil {
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
