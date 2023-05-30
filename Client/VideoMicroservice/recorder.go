package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/icza/mjpeg"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type Recorder struct {
	screenshotTool      *Screenshot
	fps                 int
	currentImg          ByteImage
	currentEncodedImage bytes.Buffer
	imageBuffer         chan bool
	VideoBuffer         chan string
	errorGroup          *errgroup.Group
	outContext          context.Context
	ctx                 context.Context
	contextCancellation context.CancelFunc
}

func NewRecorder(outContext context.Context, fps int) (*Recorder, error) {
	if fps > 60 && fps < 1 {
		return nil, errors.New("fps must be between 1 and 60")
	}

	screenshot, err := NewScreenshot()
	if err != nil {
		return nil, err
	}

	return &Recorder{
		screenshotTool:      screenshot,
		fps:                 fps,
		imageBuffer:         make(chan bool, 144),
		VideoBuffer:         make(chan string, 3),
		errorGroup:          nil,
		outContext:          outContext,
		ctx:                 nil,
		contextCancellation: nil,
		currentImg:          ByteImage{PixelSize: 4},
	}, nil
}

func (r *Recorder) Stop() {
	if r.contextCancellation != nil {
		r.contextCancellation()
	}
}

func (r *Recorder) Start(startTime time.Time, chunkSize time.Duration) {
	r.ctx, r.contextCancellation = context.WithCancel(r.outContext)
	r.errorGroup, r.ctx = errgroup.WithContext(r.ctx)

	r.errorGroup.Go(func() error {
		for time.Now().Before(startTime) && r.outContext.Err() == nil {
			time.Sleep(time.Now().Sub(startTime))
		}

		r.errorGroup.Go(r.startRecording)
		r.errorGroup.Go(func() error { return r.processImagesBuffer(startTime, chunkSize) })

		return r.errorGroup.Wait()
	})
}

func (r *Recorder) startRecording() error {
	ticker := time.NewTicker(time.Duration(int64(time.Second) / int64(r.fps)))

	for r.outContext.Err() == nil {
		s := time.Now()
		err := r.screenshotTool.Get(&r.currentImg)
		if err != nil {
			return err
		}

		r.imageBuffer <- true
		fmt.Println("CAPTURE: ", time.Since(s))
		<-ticker.C
	}

	return nil
}

func (r *Recorder) processImagesBuffer(startTime time.Time, chunkSize time.Duration) error {
	nextChunkEndTime := startTime

	for r.outContext.Err() == nil {
		nextChunkEndTime = nextChunkEndTime.Add(chunkSize)
		videoFileName := fmt.Sprintf("/tmp/%s.mkv", fmt.Sprint(nextChunkEndTime.Add(-chunkSize).UnixMilli()))
		video, err := mjpeg.New(videoFileName, int32(r.currentImg.Width), int32(r.currentImg.Height), int32(r.fps))
		if err != nil {
			fmt.Println("initing a video err", err.Error())
			return err
		}

		for r.outContext.Err() == nil && time.Now().Before(nextChunkEndTime) {
			<-r.imageBuffer
			s := time.Now()
			if err = r.currentImg.Compress(&r.currentEncodedImage, 100); err != nil {
				return err
			}
			fmt.Println("COMPRESS: ", time.Since(s))
			if err = video.AddFrame(r.currentEncodedImage.Bytes()); err != nil {
				log.Println("Error adding frame to video file ", err)
				return err
			}
			r.currentEncodedImage.Reset()
		}

		r.VideoBuffer <- videoFileName

		if err = video.Close(); err != nil {
			return err
		}
	}

	return nil
}
