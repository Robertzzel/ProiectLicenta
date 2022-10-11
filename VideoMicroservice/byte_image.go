package main

import (
	"bytes"
	"errors"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
)

type ByteImage struct {
	Data      []byte
	Width     uint
	Height    uint
	PixelSize uint
	Stride    uint
}

func (bi *ByteImage) getImage() image.Image {
	for i := 0; i < len(bi.Data); i += 4 {
		bi.Data[i], bi.Data[i+2], bi.Data[i+3] = bi.Data[i+2], bi.Data[i], 255
	}

	return &image.RGBA{
		Pix:    bi.Data,
		Stride: int(bi.Stride),
		Rect:   image.Rect(0, 0, int(bi.Width), int(bi.Height)),
	}
}

func (bi *ByteImage) SetPixel(x int, y int, r uint8, g uint8, b uint8) error {
	offset := y*int(bi.Stride) + x*int(bi.PixelSize)
	if offset >= len(bi.Data) || offset < 0 {
		return errors.New("pixel off the screen")
	}

	bi.Data[offset] = r
	bi.Data[offset+1] = g
	bi.Data[offset+2] = b

	return nil
}

func (bi *ByteImage) Compress(outputBuffer *bytes.Buffer, quality int) error {
	if quality < 1 && quality > 100 {
		return errors.New("the quality must be between 1 and 100")
	}

	return jpeg.Encode(outputBuffer, Thumbnail(854, 480, bi), &jpeg.EncoderOptions{Quality: quality})
}
