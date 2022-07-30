package main

import (
	"Licenta/kafka"
	"fmt"
	gohook "github.com/robotn/gohook"
)

type Command int

const (
	KeyDown   Command = 1
	KeyUp     Command = 2
	MouseDown Command = 3
	MouseUp   Command = 4
	MouseMove Command = 5

	LeftClick   MouseClick = 1
	MiddleClick MouseClick = 2
	RightClick  MouseClick = 3

	kafkaTopic = "input"
)

type MouseClick int
type function func([]byte)
type InputListenerService struct {
	kafkaTopic string
}

func NewInputListenerService() *InputListenerService {
	return &InputListenerService{}
}

func (ils *InputListenerService) Listen(callback function) {
	eventHook := gohook.Start()
	var event gohook.Event

	for event = range eventHook {
		switch event.Kind {
		case gohook.KeyDown:
			callback([]byte(fmt.Sprintf("%d,%d,%s\n", KeyDown, event.Rawcode, event.When.Format(kafka.TimeFormat))))
		case gohook.KeyUp:
			callback([]byte(fmt.Sprintf("%d,%d,%s\n", KeyUp, event.Rawcode, event.When.Format(kafka.TimeFormat))))
		case gohook.MouseDown:
			callback([]byte(fmt.Sprintf("%d,%d,%d,%d,%s\n", MouseDown, event.Button, event.X, event.Y, event.When.Format(kafka.TimeFormat))))
		case gohook.MouseUp:
			callback([]byte(fmt.Sprintf("%d,%d,%d,%d,%s\n", MouseUp, event.Button, event.X, event.Y, event.When.Format(kafka.TimeFormat))))
		case gohook.MouseMove:
			callback([]byte(fmt.Sprintf("%d,%d,%d,%s\n", MouseMove, event.X, event.Y, event.When.Format(kafka.TimeFormat))))
		}
	}
}

func main() {
	err := kafka.DeleteTopics([]string{kafkaTopic})
	if err != nil {
		fmt.Println(err)
	}
	err = kafka.CreateTopic(kafkaTopic)
	if err != nil {
		fmt.Println(err)
	}
	kafkaProducer := kafka.NewKafkaProducer(kafkaTopic)

	service := NewInputListenerService()
	service.Listen(func(command []byte) {
		err := kafkaProducer.Publish(command)
		if err != nil {
			fmt.Println(err)
			return
		}
	})
}
