package Audio

import (
	"errors"
	"github.com/gordonklaus/portaudio"
	"strings"
	"time"
)

type AudioGeneratorService struct {
	sampleRate         float64
	numberOfChannels   int
	secondsToRecord    float64
	latency            time.Duration
	deviceNameToRecord string
	stream             *portaudio.Stream
	buffer             []byte
}

func NewAudioGeneratorService() (*AudioGeneratorService, error) {
	ags := &AudioGeneratorService{
		sampleRate:         44100,
		numberOfChannels:   1,
		secondsToRecord:    0.5,
		latency:            0,
		deviceNameToRecord: "pulse",
	}

	ags.buffer = make([]byte, int(ags.secondsToRecord*ags.sampleRate))

	err := ags.Initialize()
	if err != nil {
		return nil, err
	}

	info, err := ags.getPulseDeviceInfo()
	if err != nil {
		return nil, err
	}

	stream, err := ags.getAudioStream(info, ags.buffer)
	if err != nil {
		return nil, err
	}

	ags.stream = stream
	return ags, nil
}

func (ags *AudioGeneratorService) GetAudioBuffer() []byte {
	return ags.buffer
}

func (ags *AudioGeneratorService) Initialize() error {
	if err := portaudio.Initialize(); err != nil {
		return err
	}
	return nil
}

func (ags *AudioGeneratorService) Terminate() error {
	if err := portaudio.Terminate(); err != nil {
		return err
	}
	return nil
}

func (ags *AudioGeneratorService) getPulseDeviceInfo() (*portaudio.DeviceInfo, error) {
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

func (ags *AudioGeneratorService) getAudioStream(deviceInfo *portaudio.DeviceInfo, buffer []byte) (*portaudio.Stream, error) {
	stream, err := portaudio.OpenStream(
		portaudio.StreamParameters{
			Input: portaudio.StreamDeviceParameters{
				Device:   deviceInfo,
				Channels: ags.numberOfChannels,
				Latency:  ags.latency,
			},
			Output:     portaudio.StreamDeviceParameters{Device: nil},
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

func (ags *AudioGeneratorService) recordStream() error {
	err := ags.stream.Start()
	if err != nil {
		return err
	}

	err = ags.stream.Read()
	if err != nil {
		return err
	}

	err = ags.stream.Stop()
	if err != nil {
		return err
	}

	return nil
}
