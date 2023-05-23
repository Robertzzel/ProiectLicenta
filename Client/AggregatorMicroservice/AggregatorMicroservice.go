package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
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

func GetNextAudioAndVideo(ctx context.Context, consumer KafkaConnection) (AudioVideoPair, error) {
	files := AudioVideoPair{}
	files.Audio = ""
	files.Video = ""

	for (files.Audio == "" || files.Video == "") && ctx.Err() == nil {
		msg, err := consumer.Consume(ctx)
		if err != nil {
			return AudioVideoPair{}, err
		}

		msgType, err := consumer.GetTypeFromMessage(msg)
		if err != nil {
			return AudioVideoPair{}, err
		}

		if msgType == "video" {
			files.Video = string(msg.Value)
		} else {
			files.Audio = string(msg.Value)
		}
	}

	return files, nil
}

func UpdateNextVideo(ctx context.Context, consumer KafkaConnection, files *AudioVideoPair) error {
	for ctx.Err() == nil {
		msg, err := consumer.Consume(ctx)
		if err != nil {
			return err
		}

		msgType, err := consumer.GetTypeFromMessage(msg)
		if err != nil {
			return err
		}

		if msgType == "video" {
			files.Video = string(msg.Value)
			break
		} else if msgType == "audio" {
			files.Audio = string(msg.Value)
		}
	}

	return nil
}

func UpdateNextAudio(ctx context.Context, consumer KafkaConnection, files *AudioVideoPair) error {
	for ctx.Err() == nil {
		msg, err := consumer.Consume(ctx)
		if err != nil {
			return err
		}

		msgType, err := consumer.GetTypeFromMessage(msg)
		if err != nil {
			return err
		}

		if msgType == "video" {
			files.Video = string(msg.Value)
		} else if msgType == "audio" {
			files.Audio = string(msg.Value)
			break
		}
	}

	return nil
}

func GetNextSyncedAudioAndVideo(ctx context.Context, consumer KafkaConnection) (AudioVideoPair, error) {
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

func CollectAudioAndVideoFiles(ctx context.Context, consumer KafkaConnection, outputChannel chan AudioVideoPair) error {
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

func CompressAndSendFiles(producer KafkaConnection, files AudioVideoPair, buffer *bytes.Buffer) error {
	err := files.CombineAndCompress(34, "pipe:1", buffer)
	if err != nil {
		return errors.New("Combine and compress error " + err.Error())
	}

	if err := producer.PublishClient(buffer.Bytes(), nil); err != nil {
		return errors.New("publish error " + err.Error())
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

	kafaConnection, err := NewKafkaConnection(brokerAddress, topic)
	if err != nil {
		panic(err)
	}

	errorGroup, ctx := errgroup.WithContext(NewContextCancelableBySignals())
	time.Sleep(time.Second * 2)

	ts := fmt.Sprint(time.Now().UnixMilli()/1000 + 1)
	if err = kafaConnection.PublishAudioStart([]byte(ts), nil); err != nil {
		panic(err)
	}
	if err = kafaConnection.PublishVideoStart([]byte(ts), nil); err != nil {
		panic(err)
	}
	kafaConnection.Flush(500)

	var combineOutput bytes.Buffer
	filesChannel := make(chan AudioVideoPair, 3)
	errorGroup.Go(func() error { return CollectAudioAndVideoFiles(ctx, kafaConnection, filesChannel) })
	errorGroup.Go(func() error {
		for {
			select {
			case filesPair := <-filesChannel:
				err := CompressAndSendFiles(kafaConnection, filesPair, &combineOutput)
				if err != nil {
					fmt.Println("Aggregaot sending file error", err)
					return err
				}
				combineOutput.Reset()
				_ = filesPair.Delete()
			case <-ctx.Done():
				return nil
			}
		}
	})

	if err = errorGroup.Wait(); err != nil {
		log.Println(err)
	}

	if err = kafaConnection.PublishClient([]byte("quit"), nil); err != nil {
		log.Println(err)
	}
	kafaConnection.Flush(1000)
	kafaConnection.Close()

	defer log.Println("Cleanup Done")
}
