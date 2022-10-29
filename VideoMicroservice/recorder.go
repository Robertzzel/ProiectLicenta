package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/icza/mjpeg"
	"log"
	"os"
	"time"
)

type Recorder struct {
	screenshotTool *Screenshot
	fps            int
	width          int32
	height         int32
	imageBuffer    chan []byte
	VideoBuffer    chan string
}

func waitUntil(t time.Time) {
	for time.Now().Before(t) {
		time.Sleep(t.Sub(time.Now()))
	}
}

func emptyChannel(c chan []byte) {
	for len(c) > 0 {
		<-c
	}
}

func NewRecorder(fps int) (*Recorder, error) {
	if fps > 60 && fps < 1 {
		return nil, errors.New("fps must be between 1 and 60")
	}

	screenshot, err := NewScreenshot()
	if err != nil {
		return nil, err
	}

	img, err := screenshot.Get()
	if err != nil {
		return nil, err
	}

	return &Recorder{
		screenshotTool: screenshot,
		fps:            fps,
		width:          int32(img.Width),
		height:         int32(img.Height),
		imageBuffer:    make(chan []byte, 256),
		VideoBuffer:    make(chan string, 10),
	}, nil
}

func (r *Recorder) Start(startTime time.Time, chunkSize time.Duration) {
	go func() {
		for time.Now().Before(startTime) {
			time.Sleep(time.Now().Sub(startTime))
		}

		go r.startRecording()
		go r.processImagesBuffer(startTime, chunkSize)
	}()
}

func (r *Recorder) startRecording() {
	ticker := time.NewTicker(time.Duration(int64(time.Second) / int64(r.fps)))

	for {
		go func(timeInitiated time.Time) {
			img, err := r.screenshotTool.Get()
			if err != nil {
				log.Println("Error while getting the screen ", err)
				return
			}

			var encodedImageBuffer bytes.Buffer
			if err := img.Compress(&encodedImageBuffer, 100); err != nil {
				log.Println("Error while compression the screen image ", err)
				return
			}

			r.imageBuffer <- encodedImageBuffer.Bytes()
		}(<-ticker.C)
	}
}

func (r *Recorder) processImagesBuffer(startTime time.Time, chunkSize time.Duration) {
	nextChunkEndTime := startTime
	cwd, _ := os.Getwd()

videoProcessingFor:
	for {
		nextChunkEndTime = nextChunkEndTime.Add(chunkSize)

		videoFileName := fmt.Sprintf("%s/videos/%s.mkv", cwd, fmt.Sprint(nextChunkEndTime.Add(-chunkSize).UnixMilli()))

		video, err := mjpeg.New(videoFileName, r.width, r.height, int32(r.fps))
		if err != nil {
			log.Println("Error initiating new video file", err)
			continue
		}

		for time.Now().Before(nextChunkEndTime) {
			if err := video.AddFrame(<-r.imageBuffer); err != nil {
				log.Println("Error adding frame to video file ", err)
				os.Remove(videoFileName)
				waitUntil(nextChunkEndTime)
				emptyChannel(r.imageBuffer)
				continue videoProcessingFor
			}
		}

		r.VideoBuffer <- videoFileName

		if err := video.Close(); err != nil {
			log.Println("Error while closing video file ", err)
		}
	}
}
