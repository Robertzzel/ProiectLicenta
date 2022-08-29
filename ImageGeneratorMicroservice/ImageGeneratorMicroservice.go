package main

import (
	"Licenta/kafka"
	"errors"
	"fmt"
	"github.com/icza/mjpeg"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	kafkaTopic   = "video"
	syncTopic    = "sync"
	syncTopic2   = "videoSync"
	syncInterval = 60
	videoSize    = time.Second
)

var mutex sync.Mutex

type VideoRecorder struct {
	buffer       [][]byte
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
	mutex.Lock()
	defer mutex.Unlock()
	return videoRecorder.startTime.Add(time.Duration(int64(time.Second/FPS) * int64(len(videoRecorder.buffer))))
}

func (videoRecorder *VideoRecorder) start() {
	go videoRecorder.startRecording()
	go videoRecorder.cleanup()
}

func (videoRecorder *VideoRecorder) cleanup() {
	for {
		if len(videoRecorder.buffer) > 10*FPS {
			mutex.Lock()
			videoRecorder.buffer = videoRecorder.buffer[6*FPS:]
			videoRecorder.startTime = videoRecorder.startTime.Add(6 * time.Second)
			mutex.Unlock()
		} else {
			time.Sleep(time.Second * 5)
		}
	}
}

func (videoRecorder *VideoRecorder) startRecording() {
	videoRecorder.startTime = time.Now()

	for {
		s := time.Now()

		checkErr(videoRecorder.videoService.GenerateImage())

		videoRecorder.buffer = append(videoRecorder.buffer, make([]byte, videoRecorder.videoService.GeneratedImage.Len()))
		copy(videoRecorder.buffer[len(videoRecorder.buffer)-1], videoRecorder.videoService.GeneratedImage.Bytes())

		log.Println(time.Second/FPS - time.Since(s))
		time.Sleep(time.Second/FPS - time.Since(s))
	}
}

func (videoRecorder *VideoRecorder) getFromBuffer(startTime time.Time, duration time.Duration) ([][]byte, error) {
	if startTime.Before(videoRecorder.startTime) {
		return nil, errors.New("requested start time before video recorder start time")
	}

	partEndTime := startTime.Add(duration)
	for videoRecorder.getEndTime().Before(partEndTime) {
		time.Sleep(videoRecorder.getEndTime().Sub(partEndTime))
	}

	size := uint(duration.Seconds() * float64(FPS))
	part := make([][]byte, size)

	mutex.Lock()
	startTimeDifferenceInSeconds := startTime.Sub(videoRecorder.startTime).Seconds()
	offset := uint(startTimeDifferenceInSeconds * float64(FPS))

	copy(part, videoRecorder.buffer[offset:offset+size])
	mutex.Unlock()

	return part, nil
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
		if err := video.AddFrame(image); err != nil {
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
		for i := 0; i < syncInterval; i++ {
			partStartTime := startTime.Add(time.Duration(int64(videoSize) * int64(i)))
			fileName := "videos/" + fmt.Sprint(partStartTime.Unix()) + ".mkv"

			checkErr(videoRecorder.createVideoFile(fileName, partStartTime, videoSize))
			checkErr(videoPublisher.Publish([]byte(fileName)))

			log.Println("video", fileName, "Total time: ", time.Now().Sub(partStartTime))
		}

		startTime, err = synchronise(syncPublisher, syncConsumer)
		checkErr(err)
	}
}
