package input

import (
	"fmt"
	gohook "github.com/robotn/gohook"
)

type InputListenerService struct {
	kafkaTopic string
}

func NewInputListenerService() *InputListenerService {
	return &InputListenerService{kafkaTopic: ""}
}

func (ils *InputListenerService) Listen() {
	eventHook := gohook.Start()
	var e gohook.Event

	for e = range eventHook {
		switch e.Kind {
		case gohook.KeyDown:
			fmt.Printf("pressed %s \n", e.String()+"\n")
		case gohook.KeyUp:
			fmt.Printf("released " + e.String() + "\n")
		case gohook.MouseDown:
			fmt.Printf("mouse down " + e.String() + "\n")
		case gohook.MouseUp:
			fmt.Printf("mouse up " + e.String() + "\n")
		case gohook.MouseMove:
			fmt.Printf("mouse move " + e.String() + "\n")
		}
	}
}
