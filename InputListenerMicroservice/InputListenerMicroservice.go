package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
	"github.com/go-vgo/robotgo"
	gohook "github.com/robotn/gohook"
	"net"
	"os"
)

type Command int

const (
	KeyDown   Command = 1
	KeyUp     Command = 2
	MouseDown Command = 3
	MouseUp   Command = 4
	MouseMove Command = 5

	socketName = "/tmp/inputListener.sock"
)

var screenWidth, screenHeight = robotgo.GetScreenSize()

func getRelativeWidth(coord int16) float32 {
	return float32(coord) / float32(screenWidth)
}

func getRelativeHeight(coord int16) float32 {
	return float32(coord) / float32(screenHeight)
}

func cherErr(err error) {
	if err != nil {
		panic(err)
	}
}

type function func([]byte)

func Listen(callback function) {
	eventHook := gohook.Start()
	var event gohook.Event

	for event = range eventHook {
		switch event.Kind {
		case gohook.KeyDown:
			callback([]byte(fmt.Sprintf("%d,%d,%d\n", KeyDown, event.Rawcode, event.When.UnixMilli())))
		case gohook.KeyUp:
			callback([]byte(fmt.Sprintf("%d,%d,%d\n", KeyUp, event.Rawcode, event.When.UnixMilli())))
		case gohook.MouseDown:
			callback([]byte(fmt.Sprintf("%d,%d,%f,%f,%d\n", MouseDown, event.Button, getRelativeWidth(event.X), getRelativeHeight(event.Y), event.When.UnixMilli())))
		case gohook.MouseUp:
			callback([]byte(fmt.Sprintf("%d,%d,%f,%f,%d\n", MouseUp, event.Button, getRelativeWidth(event.X), getRelativeHeight(event.Y), event.When.UnixMilli())))
		case gohook.MouseMove:
			callback([]byte(fmt.Sprintf("%d,%f,%f,%d\n", MouseMove, getRelativeWidth(event.X), getRelativeHeight(event.Y), event.When.UnixMilli())))
		}
	}
}

func getUiConnection() (net.Conn, error) {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		cherErr(os.Remove(socketName))
	}

	listen, err := net.Listen("tcp", "localhost:5001")
	cherErr(err)

	return listen.Accept()
}

func main() {
	uiConn, err := getUiConnection()
	cherErr(err)

	Listen(func(command []byte) {
		cherErr(SendMessage(uiConn, command))
	})
}
