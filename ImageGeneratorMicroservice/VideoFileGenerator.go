package main

import (
	"bytes"
	"github.com/go-vgo/robotgo"
	"github.com/icza/mjpeg"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
	"os"
	"time"
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

func (igs *VideoFileGenerator) GenerateVideoFile(duration time.Duration) (string, error) {
	tempFile, err := os.CreateTemp("", "temp*.mkv")
	if err != nil {
		return "", err
	}

	video, err := mjpeg.New(tempFile.Name(), resizedImageWidth, resizedImageHeight, FPS)
	if err != nil {
		return "", err
	}

	endTime := time.Now().Add(duration)
	for time.Now().Before(endTime) {
		s := time.Now()
		err := igs.GenerateImage()
		if err != nil {
			return "", err
		}

		err = video.AddFrame(igs.GeneratedImage.Bytes())
		if err != nil {
			return "", err
		}

		time.Sleep(time.Second/FPS - time.Since(s))
	}

	return tempFile.Name(), video.Close()
}
