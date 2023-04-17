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
	Width     uint
	Height    uint
	PixelSize uint
	Stride    uint
}

func (bi *ByteImage) getImage() image.Image {
	C.BGRAToRGBA((C.int)(len(bi.Data)), (*C.char)(unsafe.Pointer(&bi.Data[0])))

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

	return jpeg.Encode(outputBuffer, bi.getImage(), &jpeg.EncoderOptions{Quality: quality})
}
