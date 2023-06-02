package main

import (
	"bytes"
	"errors"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
)

type ByteImage struct {
	Data      []byte
	Width     int
	Height    int
	PixelSize int
	Stride    int
}

func (bi *ByteImage) getImage() image.Image {
	return &image.RGBA{
		Pix:    bi.Data,
		Stride: bi.Stride,
		Rect:   image.Rect(0, 0, bi.Width, bi.Height),
	}
}

func (bi *ByteImage) SetPixel(x int, y int, r uint8, g uint8, b uint8) {
	offset := y*bi.Stride + x*bi.PixelSize
	if offset >= len(bi.Data) || offset < 0 {
		return
	}

	bi.Data[offset] = r
	bi.Data[offset+1] = g
	bi.Data[offset+2] = b
}

func (bi *ByteImage) Compress(outputBuffer *bytes.Buffer, quality int) error {
	if quality < 1 && quality > 100 {
		return errors.New("the quality must be between 1 and 100")
	}

	return jpeg.Encode(outputBuffer, bi.getImage(), &jpeg.EncoderOptions{Quality: quality})
}
