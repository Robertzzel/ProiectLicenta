package main

import (
	"bytes"
	"fmt"
	"github.com/go-vgo/robotgo"
	"github.com/icza/mjpeg"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
	"time"
)

const (
	FPS                = 30
	resizedImageWidth  = 1280 // 480p: 852   720p: 1280
	resizedImageHeight = 720  //       480         720
	compressQuality    = 100
)

type VideoFileGenerator struct {
	resizeFactor        uint
	compressQuality     int
	cursorRadius        int
	screenshotGenerator *ScreenShotGenerator
	GeneratedImage      *bytes.Buffer
}

func NewVideoGenerator() (*VideoFileGenerator, error) {
	ssg, err := NewScreenshotGenerator()
	if err != nil {
		return nil, err
	}

	return &VideoFileGenerator{
		compressQuality:     compressQuality,
		cursorRadius:        5,
		screenshotGenerator: ssg,
		GeneratedImage:      new(bytes.Buffer),
	}, nil
}

func (igs *VideoFileGenerator) captureScreen() (*ByteImage, error) {
	screenImage, err := igs.screenshotGenerator.CaptureRect()
	if err != nil {
		return nil, err
	}

	return screenImage, nil
}

func (igs *VideoFileGenerator) appendCursor(screenImage *ByteImage) {
	cursorX, cursorY := igs.getCursorCoordinates()
	for i := cursorX - igs.cursorRadius; i < cursorX+igs.cursorRadius; i++ {
		for j := cursorY - igs.cursorRadius; j < cursorY+igs.cursorRadius; j++ {
			screenImage.SetPixel(i, j, 255, 255, 255)
		}
	}
}

func (igs *VideoFileGenerator) getCursorCoordinates() (int, int) {
	return robotgo.GetMousePos()
}

func (igs *VideoFileGenerator) resizeImage(screenImage *ByteImage) *image.Image {
	img := Thumbnail(resizedImageWidth, resizedImageHeight, screenImage)
	return &img
}

func (igs *VideoFileGenerator) compressImage(image *image.Image, outputBuffer *bytes.Buffer) error {
	err := jpeg.Encode(outputBuffer, *image, &jpeg.EncoderOptions{Quality: igs.compressQuality})
	if err != nil {
		return err
	}
	return nil
}

func (igs *VideoFileGenerator) GenerateImage() error {
	s := time.Now()
	img, err := igs.captureScreen()
	if err != nil {
		return err
	}
	fmt.Print("Captura: ", time.Since(s))

	s = time.Now()
	igs.appendCursor(img)
	resizedImage := igs.resizeImage(img)
	fmt.Print(" Resize: ", time.Since(s))

	s = time.Now()
	igs.GeneratedImage.Truncate(0)
	err = igs.compressImage(resizedImage, igs.GeneratedImage)
	if err != nil {
		return err
	}
	fmt.Println(" Compresie: ", time.Since(s))

	return nil
}

func (igs *VideoFileGenerator) GenerateVideoFile(fileName string, duration time.Duration) error {
	video, err := mjpeg.New(fileName, resizedImageWidth, resizedImageHeight, FPS)
	if err != nil {
		return err
	}
	defer video.Close()

	endTime := time.Now().Add(duration)
	for time.Now().Before(endTime) {
		s := time.Now()
		if err = igs.GenerateImage(); err != nil {
			return err
		}
		if err = video.AddFrame(igs.GeneratedImage.Bytes()); err != nil {
			return err
		}

		time.Sleep(time.Second/30 - time.Since(s))
	}

	return nil
}
