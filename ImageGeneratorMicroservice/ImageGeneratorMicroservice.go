package main

import (
	"bytes"
	"fmt"
	"github.com/go-vgo/robotgo"
	"github.com/nfnt/resize"
	"github.com/pixiv/go-libjpeg/jpeg"
	"github.com/vova616/screenshot"
	"image"
	"image/color"
	"time"
)

const kafkaTopic = "images"
const imagesPerSecond = 30

type ImageGeneratorService struct {
	resizeFactor    int
	compressQuality int
	cursorRadius    int
	kafkaTopic      string
	screenRectangle image.Rectangle
}

func NewImageGeneratorService() (*ImageGeneratorService, error) {
	screenRectangle, err := screenshot.ScreenRect()
	if err != nil {
		return nil, err
	}

	return &ImageGeneratorService{
		resizeFactor:    2,
		compressQuality: 80,
		cursorRadius:    5,
		kafkaTopic:      "",
		screenRectangle: screenRectangle,
	}, nil
}

func (igs *ImageGeneratorService) captureScreen() (*image.RGBA, error) {
	screenImage, err := screenshot.CaptureRect(igs.screenRectangle)
	if err != nil {
		return nil, err
	}

	return screenImage, nil
}

func (igs *ImageGeneratorService) appendCursor(screenImage *image.RGBA) {
	cursorX, cursorY := igs.getCursorCoordinates()
	for i := cursorX - igs.cursorRadius; i < cursorX+igs.cursorRadius; i++ {
		for j := cursorY - igs.cursorRadius; j < cursorY+igs.cursorRadius; j++ {
			screenImage.Set(i, j, color.White)
		}
	}
}

func (igs *ImageGeneratorService) getCursorCoordinates() (int, int) {
	return robotgo.GetMousePos()
}

func (igs *ImageGeneratorService) resizeImage(screenImage *image.RGBA) *image.Image {
	img := resize.Thumbnail(
		uint(screenImage.Bounds().Max.X/igs.resizeFactor),
		uint(screenImage.Bounds().Max.Y/igs.resizeFactor),
		screenImage, resize.NearestNeighbor,
	)
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
	s := time.Now()
	img, err := igs.captureScreen()
	if err != nil {
		return err
	}

	igs.appendCursor(img)
	resizedImage := igs.resizeImage(img)
	fmt.Println(time.Since(s))

	buffer.Truncate(0)
	err = igs.compressImage(resizedImage, buffer)
	if err != nil {
		return err
	}

	return nil
}

//func main() {
//	var imageBytes bytes.Buffer
//	kafkaProducer := kafka.NewKafkaProducer(kafkaTopic)
//	service, err := NewImageGeneratorService()
//	if err != nil {
//		fmt.Print(err)
//		return
//	}
//
//	for {
//		startTime := time.Now()
//
//		err = service.GenerateImage(&imageBytes)
//		if err != nil {
//			fmt.Println("Error on generating message", err)
//			return
//		}
//
//		go func() {
//			err = kafkaProducer.Publish(imageBytes.Bytes())
//			if err != nil {
//				fmt.Println("Error on kafka publish: ", err)
//				return
//			}
//		}()
//
//		time.Sleep(time.Second/imagesPerSecond - time.Since(startTime))
//	}
//}
