package main

import (
	"bytes"
	"errors"
	"github.com/icza/mjpeg"
	"sync"
	"time"
)

type Recorder struct {
	buffer     []*ByteImage
	startTime  time.Time
	screenshot *Screenshot
	fps        int64
}

var mutex sync.Mutex

func NewRecorder(fps int) (*Recorder, error) {
	if fps > 60 && fps < 1 {
		return nil, errors.New("fps must be between 1 and 60")
	}

	screenshot, err := NewScreenshot()
	if err != nil {
		return nil, err
	}

	return &Recorder{screenshot: screenshot, fps: int64(fps)}, nil
}

func (r *Recorder) getEndTime() time.Time {
	return r.startTime.Add(
		time.Duration(
			int64(time.Second) / r.fps * int64(len(r.buffer)),
		),
	)
}

func (r *Recorder) Start() {
	go r.startRecording()
	go r.clean()
}

func (r *Recorder) clean() {
	for {
		if len(r.buffer) > int(r.fps)*5 {
			mutex.Lock()
			r.buffer = r.buffer[2*r.fps:]
			r.startTime = r.startTime.Add(2 * time.Second)
			mutex.Unlock()
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (r *Recorder) startRecording() {
	ticker := time.NewTicker(time.Duration(int64(time.Second) / r.fps))

	for {
		<-ticker.C
		go func() {
			if r.startTime.IsZero() {
				r.startTime = time.Now()
			}

			image, err := r.screenshot.Get()
			checkErr(err)

			r.buffer = append(r.buffer, image)
		}()
	}
}

func (r *Recorder) CreateFile(fileName string, startTime time.Time, duration time.Duration) error {
	// Get needed images
	images, err := r.getFromBuffer(startTime, duration)
	if err != nil {
		return err
	}

	// Encode to png
	encodedImages := make([][]byte, len(images))
	var wg sync.WaitGroup
	wg.Add(len(images))

	for index, image := range images {
		go func(index int, image *ByteImage) {
			var encodedImageBuffer bytes.Buffer
			checkErr(image.Compress(&encodedImageBuffer, 100))
			encodedImages[index] = encodedImageBuffer.Bytes()

			wg.Done()
		}(index, image)
	}
	wg.Wait()

	// Create video file
	video, err := mjpeg.New(fileName, int32(images[0].Width), int32(images[0].Height), int32(r.fps))
	if err != nil {
		return err
	}

	for _, image := range encodedImages {
		if err := video.AddFrame(image); err != nil {
			return err
		}
	}

	return video.Close()
}

func (r *Recorder) getFromBuffer(startTime time.Time, duration time.Duration) ([]*ByteImage, error) {
	if startTime.Before(r.startTime) {
		return nil, errors.New("requested start time before video recorder start time")
	}

	if duration < 0 {
		return nil, errors.New("duration must be positive")
	}

	// Wait for the recorder to record all needed images
	partEndTime := startTime.Add(duration)
	for r.getEndTime().Before(partEndTime) {
		time.Sleep(partEndTime.Sub(r.getEndTime()))
	}

	// Compute offsets and size
	size := uint(duration.Seconds() * float64(r.fps))
	part := make([]*ByteImage, size)

	mutex.Lock()
	startTimeDifference := startTime.Sub(r.startTime).Seconds()
	offset := uint(startTimeDifference * float64(r.fps))
	copy(part, r.buffer[offset:offset+size])
	mutex.Unlock()

	return part, nil
}
