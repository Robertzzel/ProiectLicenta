package main

import "image"

type ByteImage struct {
	Data      []byte
	Width     uint
	Height    uint
	PixelSize uint
	Stride    uint
}

func NewByteImage(width uint, height uint, pixelSize uint) *ByteImage {
	return &ByteImage{
		Data:      make([]byte, width*height),
		Width:     width,
		Height:    height,
		PixelSize: pixelSize,
		Stride:    width * pixelSize,
	}
}

func (bi *ByteImage) getStride() uint {
	return bi.Stride
}

func (bi *ByteImage) getPixelsAfter(offset uint) []byte {
	return bi.Data[offset:]
}

func (bi *ByteImage) getImageRGBA() *image.RGBA {
	for i := 0; i < len(bi.Data); i += 4 {
		bi.Data[i], bi.Data[i+2], bi.Data[i+3] = bi.Data[i+2], bi.Data[i], 255
	}

	return &image.RGBA{bi.Data, int(bi.getStride()), image.Rect(0, 0, int(bi.Width), int(bi.Height))}
}

func (bi *ByteImage) SetPixel(x int, y int, r uint8, g uint8, b uint8) {
	offset := y*int(bi.Stride) + x*int(bi.PixelSize)
	bi.Data[offset] = r
	bi.Data[offset+1] = g
	bi.Data[offset+2] = b
}
