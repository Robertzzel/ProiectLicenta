package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"github.com/go-vgo/robotgo"
	"net"
	"strconv"
	"strings"
)

const (
	inputExecutorSocket = "/tmp/inputExecutor.sock"
	KeyDown             = "1"
	KeyUp               = "2"
	MouseDown           = "3"
	MouseUp             = "4"
	MouseMove           = "5"
	LeftClick           = "1"
	MiddleClick         = "2"
	RightClick          = "3"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func getButtonFromCode(code string) (string, error) {
	switch code {
	case LeftClick:
		return "left", nil
	case MiddleClick:
		return "middle", nil
	case RightClick:
		return "right", nil
	default:
		return "", errors.New("not supported click button")
	}
}

func getRouterConnection() (net.Conn, error) {
	listener, err := net.Listen("unix", inputExecutorSocket)
	if err != nil {
		return nil, err
	}

	return listener.Accept()
}

func main() {
	routerConn, err := getRouterConnection()
	checkErr(err)
	screenWidth, screenHeight := robotgo.GetScreenSize()

	for {
		message, err := ReceiveMessage(routerConn)
		checkErr(err)

		components := strings.Split(string(message), ",")
		switch components[0] {
		case KeyDown:
			key, err := strconv.Atoi(components[1])
			checkErr(err)
			checkErr(robotgo.KeyDown(string(key)))
		case KeyUp:
			key, err := strconv.Atoi(components[1])
			checkErr(err)
			checkErr(robotgo.KeyUp(string(key)))
		case MouseDown:
			button, err := getButtonFromCode(components[1])
			checkErr(err)

			checkErr(robotgo.MouseDown(button))
		case MouseUp:
			button, err := getButtonFromCode(components[1])
			checkErr(err)

			checkErr(robotgo.MouseUp(button))
		case MouseMove:
			x, err := strconv.ParseFloat(components[1], 32)
			checkErr(err)

			y, err := strconv.ParseFloat(components[2], 32)
			checkErr(err)

			robotgo.Move(int(x*float64(screenWidth)), int(y*float64(screenHeight)))
		}
	}
}
