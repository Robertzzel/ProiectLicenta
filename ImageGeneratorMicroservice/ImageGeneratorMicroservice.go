package main

import (
	"Licenta/kafka"
	"bytes"
	"context"
	"fmt"
	"github.com/go-vgo/robotgo"
	"github.com/icza/mjpeg"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
	"os/exec"
	"time"
)

const (
	kafkaTopic          = "video"
	syncTopic           = "sync"
	FPS                 = 30
	resizedImageWidth   = 1280
	resizedImageHeight  = 720
	videoFileName       = "auxVideo.avi"
	outputVideoFileName = "video.avi"
	videoSize           = time.Second
	compressQuality     = 100
)

type ImageGeneratorService struct {
	resizeFactor        uint
	compressQuality     int
	cursorRadius        int
	screenshotGenerator *ScreenShotGenerator
	GeneratedImage      *bytes.Buffer
}

func NewImageGeneratorService() (*ImageGeneratorService, error) {
	ssg, err := NewScreenshotGenerator()
	if err != nil {
		return nil, err
	}

	return &ImageGeneratorService{
		compressQuality:     compressQuality,
		cursorRadius:        5,
		screenshotGenerator: ssg,
		GeneratedImage:      new(bytes.Buffer),
	}, nil
}

func (igs *ImageGeneratorService) captureScreen() (*ByteImage, error) {
	screenImage, err := igs.screenshotGenerator.CaptureRect()
	if err != nil {
		return nil, err
	}

	return screenImage, nil
}

func (igs *ImageGeneratorService) appendCursor(screenImage *ByteImage) {
	cursorX, cursorY := igs.getCursorCoordinates()
	for i := cursorX - igs.cursorRadius; i < cursorX+igs.cursorRadius; i++ {
		for j := cursorY - igs.cursorRadius; j < cursorY+igs.cursorRadius; j++ {
			screenImage.SetPixel(i, j, 255, 255, 255)
		}
	}
}

func (igs *ImageGeneratorService) getCursorCoordinates() (int, int) {
	return robotgo.GetMousePos()
}

func (igs *ImageGeneratorService) resizeImage(screenImage *ByteImage) *image.Image {
	img := Thumbnail(resizedImageWidth, resizedImageHeight, screenImage)
	return &img
}

func (igs *ImageGeneratorService) compressImage(image *image.Image, outputBuffer *bytes.Buffer) error {
	err := jpeg.Encode(outputBuffer, *image, &jpeg.EncoderOptions{Quality: igs.compressQuality})
	if err != nil {
		return err
	}
	return nil
}

func (igs *ImageGeneratorService) GenerateImage() error {
	img, err := igs.captureScreen()
	if err != nil {
		return err
	}

	igs.appendCursor(img)
	resizedImage := igs.resizeImage(img)

	igs.GeneratedImage.Truncate(0)
	err = igs.compressImage(resizedImage, igs.GeneratedImage)
	if err != nil {
		return err
	}

	return nil
}

func (igs *ImageGeneratorService) GenerateVideo(duration time.Duration) error {
	video, err := mjpeg.New(videoFileName, resizedImageWidth, resizedImageHeight, FPS)
	if err != nil {
		return err
	}

	endTime := time.Now().Add(duration)
	for time.Now().Before(endTime) {
		s := time.Now()
		err := igs.GenerateImage()
		if err != nil {
			return err
		}

		err = video.AddFrame(igs.GeneratedImage.Bytes())
		if err != nil {
			return err
		}

		time.Sleep(time.Second/FPS - time.Since(s))
	}

	return video.Close()
}

func main() {
	err := kafka.CreateTopic(kafkaTopic)
	if err != nil {
		fmt.Println(err)
		return
	}
	kafkaProducer := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncConsumer := kafka.NewKafkaConsumer(syncTopic)
	if err = syncConsumer.Reader.SetOffsetAt(context.Background(), time.Now()); err != nil {
		fmt.Println(err)
		return
	}

	service, err := NewImageGeneratorService()
	if err != nil {
		fmt.Print(err)
		return
	}

	for {
		syncMsg, err := syncConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			return
		}

		go func() {
			fmt.Println(string(syncMsg.Value), time.Now())
			err = service.GenerateVideo(videoSize)
			if err != nil {
				fmt.Println(err)
				return
			}

			err = exec.Command("mv", videoFileName, outputVideoFileName).Run()
			if err != nil {
				fmt.Println(err)
				return
			}

			err = kafkaProducer.Publish([]byte("."))
			if err != nil {
				fmt.Println(err)
				return
			}
		}()
	}
}
