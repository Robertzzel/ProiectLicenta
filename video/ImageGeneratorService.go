package video

import (
	"bytes"
	"github.com/go-vgo/robotgo"
	"github.com/nfnt/resize"
	"github.com/pixiv/go-libjpeg/jpeg"
	"github.com/vova616/screenshot"
	"image"
	"image/color"
)

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

func (igs *ImageGeneratorService) GenerateImage(buffer *bytes.Buffer) (*image.Image, error) {
	img, err := igs.captureScreen()
	if err != nil {
		return nil, err
	}

	igs.appendCursor(img)
	resizedImage := igs.resizeImage(img)

	buffer.Truncate(0)
	err = igs.compressImage(resizedImage, buffer)
	if err != nil {
		return nil, err
	}

	return resizedImage, nil
}
