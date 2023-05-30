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

func (igs *Screenshot) Get(bi *ByteImage) error {
	err := igs.screenshot.Capture()
	if err == true {
		return errors.New("Failed to capture img")
	}

	bi.Data = igs.screenshot.Image
	bi.Width = igs.screenshot.Width
	bi.Height = igs.screenshot.Height
	bi.Stride = 4 * igs.screenshot.Width

	igs.appendCursor(bi)
	return nil
}

func (igs *Screenshot) appendCursor(screenImage *ByteImage) {
	cursorX, cursorY := robotgo.GetMousePos()
	for i := cursorX - igs.cursorRadius; i < cursorX+igs.cursorRadius; i++ {
		for j := cursorY - igs.cursorRadius; j < cursorY+igs.cursorRadius; j++ {
			screenImage.SetPixel(i, j, 255, 255, 255)
		}
	}
}
