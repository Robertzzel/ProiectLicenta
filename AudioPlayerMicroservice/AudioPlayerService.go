package main

import (
	"Licenta/kafka"
	"fmt"
	"github.com/gordonklaus/portaudio"
)

const (
	kafkaAudioTopic  = "rAudio"
	sampleRate       = 44100
	numberOfChannels = 1
)

func main() {
	var err error
	audioConsumer := kafka.NewKafkaConsumer(kafkaAudioTopic)
	if err != nil {
		return
	}

	err = portaudio.Initialize()
	if err != nil {
		return
	}
	defer portaudio.Terminate()

	buffer := make([]byte, numberOfChannels*sampleRate)

	audioStream, err := portaudio.OpenDefaultStream(0, numberOfChannels, sampleRate, len(buffer), buffer)
	if err != nil {
		return
	}

	err = audioStream.Start()
	if err != nil {
		return
	}
	defer audioStream.Stop()

	for {
		message, err := audioConsumer.Consume()
		if err != nil {
			fmt.Println(err)
			break
		}

		buffer = message.Value
		err = audioStream.Write()
		if err != nil {
			return
		}
	}
}
