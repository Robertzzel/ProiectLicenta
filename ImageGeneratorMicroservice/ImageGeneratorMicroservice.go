package main

import (
	"Licenta/kafka"
	"bytes"
	"fmt"
	"github.com/go-vgo/robotgo"
	"github.com/icza/mjpeg"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
	"os"
	"os/exec"
	"sync"
	"time"
)

const (
	kafkaTopic         = "video"
	syncTopic          = "sync"
	FPS                = 30
	resizedImageWidth  = 1280
	resizedImageHeight = 720
	videoSize          = time.Second
	compressQuality    = 100
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

func (igs *ImageGeneratorService) GenerateVideoFile(duration time.Duration) (*os.File, error) {
	tempFile, err := os.CreateTemp("", "temp")
	if err != nil {
		return nil, err
	}

	video, err := mjpeg.New(tempFile.Name(), resizedImageWidth, resizedImageHeight, FPS)
	if err != nil {
		return nil, err
	}

	endTime := time.Now().Add(duration)
	for time.Now().Before(endTime) {
		s := time.Now()
		err := igs.GenerateImage()
		if err != nil {
			return nil, err
		}

		err = video.AddFrame(igs.GeneratedImage.Bytes())
		if err != nil {
			return nil, err
		}

		time.Sleep(time.Second/FPS - time.Since(s))
	}

	return tempFile, video.Close()
}

func main() {
	err := kafka.CreateTopic(kafkaTopic)
	if err != nil {
		fmt.Println(err)
		return
	}
	kafkaProducer := kafka.NewVideoKafkaProducer(kafkaTopic)
	syncConsumer := kafka.NewKafkaConsumer(syncTopic)
	if err = syncConsumer.SetOffsetToNow(); err != nil {
		fmt.Println(err)
		return
	}

	service, err := NewImageGeneratorService()
	if err != nil {
		fmt.Print(err)
		return
	}

	var mutex sync.Mutex
	for {
		syncMsg, err := syncConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(string(syncMsg.Value), time.Now())

		go func() {
			compressedFile, err := os.CreateTemp("", "output")
			if err != nil {
				fmt.Println(err)
				return
			}
			defer func() {
				compressedFile.Close()
				os.Remove(compressedFile.Name())
			}()

			mutex.Lock()
			videoFile, err := service.GenerateVideoFile(videoSize)
			if err != nil {
				fmt.Println(err)
				return
			}
			mutex.Unlock()
			defer func() {
				videoFile.Close()
				os.Remove(videoFile.Name())
			}()

			if out, err := exec.Command("./CompressFile", videoFile.Name(), compressedFile.Name(), "30").Output(); err != nil {
				fmt.Println(err, out)
				return
			}

			file, err := os.ReadFile(compressedFile.Name())
			if err != nil {
				fmt.Println(err)
				return
			}

			err = kafkaProducer.Publish(file)
			if err != nil {
				fmt.Println(err)
				return
			}
		}()
	}
}
