package main

import (
	"Licenta/kafka"
	"bytes"
	"fmt"
	"github.com/go-vgo/robotgo"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
	"time"
)

const kafkaTopic = "images"
const imagesPerSecond = 30

type ImageGeneratorService struct {
	resizeFactor        uint
	compressQuality     int
	cursorRadius        int
	screenshotGenerator *ScreenShotGenerator
}

func NewImageGeneratorService() (*ImageGeneratorService, error) {
	ssg, err := NewScreenshotGenerator()
	if err != nil {
		return nil, err
	}

	return &ImageGeneratorService{
		resizeFactor:        2,
		compressQuality:     80,
		cursorRadius:        5,
		screenshotGenerator: ssg,
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
	img := Thumbnail(screenImage.Width/igs.resizeFactor, screenImage.Height/igs.resizeFactor, screenImage)
	return &img
}

func (igs *ImageGeneratorService) compressImage(image *image.Image, outputBuffer *bytes.Buffer) error {
	err := jpeg.Encode(outputBuffer, *image, &jpeg.EncoderOptions{Quality: igs.compressQuality})
	if err != nil {
		return err
	}
	return nil
}

func (igs *ImageGeneratorService) GenerateImage(buffer *bytes.Buffer) error {
	img, err := igs.captureScreen()
	if err != nil {
		return err
	}

	igs.appendCursor(img)
	resizedImage := igs.resizeImage(img)

	buffer.Truncate(0)
	err = igs.compressImage(resizedImage, buffer)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	err := kafka.CreateTopic(kafkaTopic, 1)
	if err != nil {
		fmt.Println(err)
		return
	}

	var imageBytes bytes.Buffer
	kafkaProducer := kafka.NewKafkaProducer(kafkaTopic)
	service, err := NewImageGeneratorService()
	if err != nil {
		fmt.Print(err)
		return
	}

	for {
		startTime := time.Now()

		err = service.GenerateImage(&imageBytes)
		if err != nil {
			fmt.Println("Error on generating message", err)
			return
		}

		go func() {
			err = kafkaProducer.Publish(imageBytes.Bytes())
			if err != nil {
				fmt.Println("Error on kafka publish: ", err)
				return
			}
		}()

		time.Sleep(time.Second/imagesPerSecond - time.Since(startTime))
	}
}
