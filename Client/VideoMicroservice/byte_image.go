package main

/*
void BGRAToRGBA(int width, char* img){
	int aux, i;
	for(i = 0; i < width; i += 4){
		aux = img[i];
		img[i] = img[i+2];
		img[i+2] = aux;
		img[i+3] = 255;
	}
}
*/
import "C"
import (
	"bytes"
	"errors"
	"github.com/pixiv/go-libjpeg/jpeg"
	"image"
	"unsafe"
)

type ByteImage struct {
	Data      []byte
	Width     int
	Height    int
	PixelSize int
	Stride    int
}

func (bi *ByteImage) getImage() image.Image {
	C.BGRAToRGBA((C.int)(len(bi.Data)), (*C.char)(unsafe.Pointer(&bi.Data[0])))

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
