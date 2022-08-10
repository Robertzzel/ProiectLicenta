package main

import (
	"Licenta/kafka"
	"bytes"
	"errors"
	"fmt"
	"github.com/gordonklaus/portaudio"
	"strings"
)

const (
	kafkaAudioTopic  = "audio"
	secondsToRecord  = 1.0 / 6
	sampleRate       = 44100
	numberOfChannels = 1
)

type AudioGeneratorService struct {
	Stream      *portaudio.Stream
	AudioBuffer *bytes.Buffer
}

func NewAudioGeneratorService(stream *portaudio.Stream) (*AudioGeneratorService, error) {
	ags := &AudioGeneratorService{}
	ags.AudioBuffer = bytes.NewBuffer(make([]byte, int(float64(sampleRate)*secondsToRecord)))
	var err error

	if stream != nil {
		ags.Stream = stream
		return ags, nil
	}

	ags.Stream, err = portaudio.OpenDefaultStream(numberOfChannels, 0, sampleRate, ags.AudioBuffer.Len(), ags.AudioBuffer.Bytes())
	if err != nil {
		return nil, err
	}

	return ags, nil
}

func (ags *AudioGeneratorService) RecordStream() error {
	return ags.Stream.Read()
}

func GetDeviceInfoByName(name string) (*portaudio.DeviceInfo, error) {
	devices, err := portaudio.Devices()
	if err != nil {
		return nil, err
	}

	for _, device := range devices {
		if strings.Contains(device.Name, name) {
			return device, nil
		}
	}

	return nil, errors.New("no pulse device found")
}

func GetAudioStream(deviceInfo *portaudio.DeviceInfo) (*portaudio.Stream, *bytes.Buffer, error) {
	buffer := bytes.NewBuffer(make([]byte, int(float64(sampleRate)*secondsToRecord)))

	err := portaudio.Initialize()
	if err != nil {
		return nil, nil, err
	}

	stream, err := portaudio.OpenStream(
		portaudio.StreamParameters{
			Input: portaudio.StreamDeviceParameters{
				Device:   deviceInfo,
				Channels: numberOfChannels,
				Latency:  0,
			},
			Output:     portaudio.StreamDeviceParameters{Device: nil},
			Flags:      portaudio.NoFlag,
			SampleRate: sampleRate,
		},
		buffer.Bytes(),
	)
	if err != nil {
		return nil, nil, err
	}

	err = portaudio.Terminate()
	if err != nil {
		return nil, nil, err
	}

	return stream, buffer, nil
}

func main() {
	portaudio.Initialize()

	kafkaPublisher := kafka.NewImageKafkaProducer(kafkaAudioTopic)
	service, err := NewAudioGeneratorService(nil)
	if err != nil {
		return
	}

	err = service.Stream.Start()
	if err != nil {
		return
	}

	for {
		err = service.Stream.Read()
		if err != nil {
			fmt.Println(err)
			break
		}

		kafkaPublisher.PublishWithTimestamp(service.AudioBuffer.Bytes())
	}

	err = service.Stream.Stop()
	if err != nil {
		return
	}

	err = portaudio.Terminate()
	if err != nil {
		return
	}
}
