package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/icza/mjpeg"
	"os"
	"time"
)

type Recorder struct {
	screenshotTool *Screenshot
	fps            int
	width          int
	height         int
	imageBuffer    chan []byte
	VideoBuffer    chan string
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
		width:          int(img.Width),
		height:         int(img.Height),
	}, nil
}

func (r *Recorder) Start(startTime time.Time, chunkSize time.Duration) {
	r.imageBuffer = make(chan []byte, 256)
	r.VideoBuffer = make(chan string, 10)
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
			checkErr(err)

			var encodedImageBuffer bytes.Buffer
			checkErr(img.Compress(&encodedImageBuffer, 100))

			r.imageBuffer <- encodedImageBuffer.Bytes()
		}(<-ticker.C)
	}
}

func (r *Recorder) processImagesBuffer(startTime time.Time, chunkSize time.Duration) {
	nextChunkEndTime := startTime.Add(chunkSize)
	cwd, _ := os.Getwd()

	for {
		videoFileName := fmt.Sprintf("%s/videos/%s.mkv", cwd, fmt.Sprint(nextChunkEndTime.Add(-chunkSize).Unix()))

		video, err := mjpeg.New(
			videoFileName,
			int32(r.width),
			int32(r.height),
			int32(r.fps),
		)
		checkErr(err)

		for time.Now().Before(nextChunkEndTime) {
			checkErr(video.AddFrame(<-r.imageBuffer))
		}

		checkErr(video.Close())
		r.VideoBuffer <- videoFileName

		nextChunkEndTime = nextChunkEndTime.Add(chunkSize)
	}
}
