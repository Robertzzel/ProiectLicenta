package main

import (
	"github.com/BurntSushi/xgb"
	"github.com/BurntSushi/xgb/xproto"
	"sync"
)

var screenMutex sync.Mutex

type Screen struct {
	connection *xgb.Conn
	screen     *xproto.ScreenInfo
	width      uint16
	height     uint16
	minWidth   int16
	minHeight  int16
	drawable   xproto.Drawable
}

func NewScreen() (*Screen, error) {
	c, err := xgb.NewConn()
	if err != nil {
		return nil, err
	}

	screen := xproto.Setup(c).DefaultScreen(c)

	return &Screen{
		connection: c,
		screen:     screen,
		width:      screen.WidthInPixels,
		height:     screen.HeightInPixels,
		minWidth:   0,
		minHeight:  0,
		drawable:   xproto.Drawable(screen.Root),
	}, nil
}

func (ig *Screen) Get() (*ByteImage, error) {
	screenMutex.Lock()
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
	screenMutex.Unlock()

	return &ByteImage{
		Data:      xImg.Data,
		Width:     uint(ig.width),
		Height:    uint(ig.height),
		PixelSize: 4,
		Stride:    uint(ig.width * 4),
	}, nil
}

func (ig *Screen) Close() {
	ig.connection.Close()
}
