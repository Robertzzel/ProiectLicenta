package main

import (
	"errors"
	"github.com/go-vgo/robotgo"
)

const (
	cursorRadius = 5
)

type Screenshot struct {
	cursorRadius int
	screenshot   Screen
}

func NewScreenshot() (*Screenshot, error) {
	ssg, err := NewScreen()
	if err != nil {
		return nil, err
	}

	return &Screenshot{
		cursorRadius: cursorRadius,
		screenshot:   ssg,
	}, nil
}

func (igs *Screenshot) Get() (*ByteImage, error) {
	err := igs.screenshot.Capture()
	if err == true {
		return nil, errors.New("Failed to capture img")
	}

	img := ByteImage{
		Data:      igs.screenshot.Image,
		Width:     uint(igs.screenshot.Width),
		Height:    uint(igs.screenshot.Height),
		PixelSize: 4,
		Stride:    uint(igs.screenshot.Width * 4),
	}
	if err := igs.appendCursor(&img); err != nil {
		return nil, err
	}

	return &img, nil
}

func (igs *Screenshot) appendCursor(screenImage *ByteImage) error {
	cursorX, cursorY := robotgo.GetMousePos()
	for i := cursorX - igs.cursorRadius; i < cursorX+igs.cursorRadius; i++ {
		for j := cursorY - igs.cursorRadius; j < cursorY+igs.cursorRadius; j++ {
			// Ignoring error because cursor may not be on screen
			screenImage.SetPixel(i, j, 255, 255, 255)
		}
	}

	return nil
}
