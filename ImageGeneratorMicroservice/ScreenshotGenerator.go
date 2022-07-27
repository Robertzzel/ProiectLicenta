package main

import (
	"github.com/BurntSushi/xgb"
	"github.com/BurntSushi/xgb/xproto"
	"image"
)

type ScreenShotGenerator struct {
	ScreenRectangle image.Rectangle
	connection      *xgb.Conn
	screen          *xproto.ScreenInfo
	width           uint16
	height          uint16
	minWidth        int16
	minHeight       int16
	drawable        xproto.Drawable
}

func NewScreenshotGenerator() (*ScreenShotGenerator, error) {
	c, err := xgb.NewConn()
	if err != nil {
		return nil, err
	}

	screen := xproto.Setup(c).DefaultScreen(c)
	width := screen.WidthInPixels
	height := screen.HeightInPixels
	screenRect := image.Rect(0, 0, int(width), int(height))

	return &ScreenShotGenerator{
		ScreenRectangle: screenRect,
		connection:      c,
		screen:          screen,
		width:           width,
		height:          height,
		minWidth:        int16(screenRect.Min.X),
		minHeight:       int16(screenRect.Min.Y),
		drawable:        xproto.Drawable(screen.Root),
	}, nil
}

func (ig *ScreenShotGenerator) CaptureRect() (*ByteImage, error) {
	xImg, err := xproto.GetImage(
		ig.connection,
		xproto.ImageFormatZPixmap,
		ig.drawable,
		ig.minWidth,
		ig.minHeight,
		ig.width, ig.height,
		0xffffffff,
	).Reply()
	if err != nil {
		return nil, err
	}

	return &ByteImage{
		Data:      xImg.Data,
		Width:     uint(ig.width),
		Height:    uint(ig.height),
		PixelSize: 4,
		Stride:    uint(ig.width * 4),
	}, nil
}

func (ig *ScreenShotGenerator) Close() {
	ig.connection.Close()
}
