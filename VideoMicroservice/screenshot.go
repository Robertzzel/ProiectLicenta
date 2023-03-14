package main

import (
	"github.com/go-vgo/robotgo"
)

const (
	cursorRadius = 3
)

type Screenshot struct {
	cursorRadius int
	screenshot   *Screen
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
	img, err := igs.screenshot.Get()
	if err != nil {
		return nil, err
	}

	if err = igs.appendCursor(img); err != nil {
		return nil, err
	}

	return img, nil
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
