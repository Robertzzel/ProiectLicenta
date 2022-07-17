package main

import (
	"errors"
	"github.com/gordonklaus/portaudio"
	"strings"
	"time"
)

type AudioPlayerService struct {
	sampleRate         float64
	numberOfChannels   int
	secondsToRecord    uint
	latency            time.Duration
	deviceNameToRecord string
}

func NewAudioPlayerService() *AudioPlayerService {
	return &AudioPlayerService{
		sampleRate:         44100 / 4,
		numberOfChannels:   1,
		secondsToRecord:    3,
		latency:            0,
		deviceNameToRecord: "pulse",
	}
}

func (ags *AudioPlayerService) initialize() error {
	if err := portaudio.Initialize(); err != nil {
		return err
	}
	return nil
}

func (ags *AudioPlayerService) terminate() error {
	if err := portaudio.Terminate(); err != nil {
		return err
	}
	return nil
}

func (ags *AudioPlayerService) getPulseDeviceInfo() (*portaudio.DeviceInfo, error) {
	devices, err := portaudio.Devices()
	if err != nil {
		return nil, err
	}

	for _, device := range devices {
		if strings.Contains(device.Name, ags.deviceNameToRecord) {
			return device, nil
		}
	}

	return nil, errors.New("no pulse device found")
}

func (ags *AudioPlayerService) getAudioStream(deviceInfo *portaudio.DeviceInfo, buffer []byte) (*portaudio.Stream, error) {
	stream, err := portaudio.OpenStream(
		portaudio.StreamParameters{
			Input: portaudio.StreamDeviceParameters{Device: nil},
			Output: portaudio.StreamDeviceParameters{
				Device:   deviceInfo,
				Channels: ags.numberOfChannels,
				Latency:  ags.latency,
			},
			Flags:      portaudio.NoFlag,
			SampleRate: ags.sampleRate,
		},
		buffer,
	)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (ags *AudioPlayerService) playAudio(stream *portaudio.Stream) error {
	err := stream.Start()
	if err != nil {
		return err
	}

	err = stream.Write()
	if err != nil {
		return err
	}

	err = stream.Stop()
	if err != nil {
		return err
	}

	return nil
}
