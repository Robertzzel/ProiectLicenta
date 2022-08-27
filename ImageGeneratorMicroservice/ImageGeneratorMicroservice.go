package main

import (
	"Licenta/kafka"
	"bytes"
	"errors"
	"fmt"
	"github.com/icza/mjpeg"
	"strconv"
	"time"
)

const (
	kafkaTopic = "video"
	syncTopic  = "sync"
	syncTopic2 = "videoSync"
)

type VideoRecorder struct {
	buffer       []bytes.Buffer
	startTime    time.Time
	videoService *VideoFileGenerator
}

func NewVideoRecorder() (*VideoRecorder, error) {
	videoGenerator, err := NewVideoGenerator()
	if err != nil {
		return nil, err
	}

	return &VideoRecorder{
		videoService: videoGenerator,
	}, nil
}
func (videoRecorder *VideoRecorder) getEndTime() time.Time {
	return videoRecorder.startTime.Add(time.Duration(int64(time.Second/FPS) * int64(len(videoRecorder.buffer))))
}

func (videoRecorder *VideoRecorder) start() {
	go videoRecorder.startRecording()
	go videoRecorder.autoCleanup()
}

func (videoRecorder *VideoRecorder) startRecording() {
	videoRecorder.startTime = time.Now()

	for {
		s := time.Now()

		checkErr(videoRecorder.videoService.GenerateImage())

		newBuffer := *bytes.NewBuffer([]byte(""))
		newBuffer.Reset()
		newBuffer.Write(videoRecorder.videoService.GeneratedImage.Bytes())

		videoRecorder.buffer = append(videoRecorder.buffer, newBuffer)
		time.Sleep(time.Second/FPS - time.Since(s))
	}
}

func (videoRecorder *VideoRecorder) autoCleanup() {
	for {
		if len(videoRecorder.buffer) > FPS*6 {
			videoRecorder.buffer = videoRecorder.buffer[FPS:]
			videoRecorder.startTime = videoRecorder.startTime.Add(time.Second)
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (videoRecorder *VideoRecorder) getFromBuffer(startTime time.Time, duration time.Duration) ([]bytes.Buffer, error) {
	if startTime.Before(videoRecorder.startTime) {
		return nil, errors.New("requested start time before video recorder start time")
	}

	endTimeDifference := videoRecorder.getEndTime().Sub(startTime.Add(duration))
	if endTimeDifference < 0 {
		time.Sleep(-endTimeDifference + time.Second/FPS)
	}

	startTimeDifferenceInSeconds := startTime.Sub(videoRecorder.startTime).Seconds()
	offset := uint(startTimeDifferenceInSeconds * float64(FPS))
	size := uint(duration.Seconds() * float64(FPS))

	return videoRecorder.buffer[offset : offset+size], nil
}

func (videoRecorder *VideoRecorder) createVideoFile(fileName string, startTime time.Time, duration time.Duration) error {
	images, err := videoRecorder.getFromBuffer(startTime, duration)
	if err != nil {
		return err
	}

	video, err := mjpeg.New(fileName, resizedImageWidth, resizedImageHeight, FPS)
	if err != nil {
		return err
	}
	defer video.Close()

	for _, image := range images {
		if err := video.AddFrame(image.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func synchronise(syncPublisher *kafka.Producer, syncConsumer *kafka.Consumer) (time.Time, error) {
	if err := syncPublisher.Publish([]byte(".")); err != nil {
		return time.Time{}, err
	}

	syncMsg, err := syncConsumer.Consume()
	if err != nil {
		return time.Now(), err
	}

	timestamp, err := strconv.ParseInt(string(syncMsg.Value), 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(timestamp, 0), nil
}

func main() {
	checkErr(kafka.CreateTopic(kafkaTopic))

	videoPublisher := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncPublisher := kafka.NewSyncKafkaProducer(syncTopic2)
	syncConsumer := kafka.NewKafkaConsumer(syncTopic)

	checkErr(syncConsumer.SetOffsetToNow())

	videoRecorder, err := NewVideoRecorder()
	checkErr(err)

	videoRecorder.start()
	startTime, err := synchronise(syncPublisher, syncConsumer)
	checkErr(err)

	for {
		for i := 0; i < 15; i++ {
			fileName := "videos/" + fmt.Sprint(time.Now().Unix()) + ".mkv"

			s := time.Now()
			checkErr(videoRecorder.createVideoFile(fileName, startTime.Add(time.Duration(int64(time.Second)*int64(2*i))), time.Second*2))
			checkErr(videoPublisher.Publish([]byte(fileName)))

			fmt.Println("video ", time.Now().Unix(), time.Since(s))
		}

		startTime, err = synchronise(syncPublisher, syncConsumer)
		checkErr(err)
	}
}
