package main

import (
	"errors"
	"github.com/gordonklaus/portaudio"
	"strings"
	"time"
)

type AudioGeneratorService struct {
	sampleRate         float64
	numberOfChannels   int
	secondsToRecord    uint
	latency            time.Duration
	deviceNameToRecord string
}

func NewAudioGeneratorService() *AudioGeneratorService {
	return &AudioGeneratorService{
		sampleRate:         44100,
		numberOfChannels:   1,
		secondsToRecord:    3,
		latency:            0,
		deviceNameToRecord: "pulse",
	}
}

func (ags *AudioGeneratorService) initialize() error {
	if err := portaudio.Initialize(); err != nil {
		return err
	}
	return nil
}

func (ags *AudioGeneratorService) terminate() error {
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

func (ags *AudioGeneratorService) recordStream(stream *portaudio.Stream) error {
	err := stream.Start()
	if err != nil {
		return err
	}

	err = stream.Read()
	if err != nil {
		return err
	}

	err = stream.Stop()
	if err != nil {
		return err
	}

	return nil
}

//func main() {
//	service := NewAudioGeneratorService()
//	err := service.Initialize()
//	if err != nil {
//		return
//	}
//	defer service.Terminate()
//
//	var buffer = make([]byte, 4*44100)
//	pulseDeviceInfo, err := service.getPulseDeviceInfo()
//	if err != nil {
//		return
//	}
//
//	stream, err := service.getAudioStream(pulseDeviceInfo, buffer)
//	if err != nil {
//		return
//	}
//	err = service.recordStream(stream)
//	if err != nil {
//		return
//	}
//
//	pulseOut, err := service.getOutputAudioStream(pulseDeviceInfo, buffer)
//	if err != nil {
//		return
//	}
//
//	err = service.playAudio(pulseOut)
//	if err != nil {
//		return
//	}
//}

//func main() {
//	err := portaudio.Initialize()
//	if err != nil {
//		return
//	}
//	defer portaudio.Terminate()
//
//	devices, err := portaudio.Devices()
//	if err != nil {
//		fmt.Println("Eroare la deviceuri")
//		return
//	}
//
//	var deviceInfo *portaudio.DeviceInfo
//	for _, device := range devices {
//		if strings.Contains(device.Name, "pulse") {
//			deviceInfo = device
//		}
//	}
//
//	var buffer1 = make([]byte, 44100*2)
//
//	stream, err := portaudio.OpenStream(
//		portaudio.StreamParameters{
//			Input:      portaudio.StreamDeviceParameters{Device: deviceInfo, Channels: 1, Latency: 0},
//			Output:     portaudio.StreamDeviceParameters{Device: nil},
//			Flags:      portaudio.NoFlag,
//			SampleRate: 44100,
//		},
//		buffer1,
//	)
//	if err != nil {
//		fmt.Println("Pb la stream")
//		fmt.Println(err)
//		return
//	}
//
//	s := time.Now()
//	if err := stream.Start(); err != nil {
//		fmt.Println("Eroare la start")
//		fmt.Println(err)
//		return
//	}
//
//	nr, err := stream.AvailableToRead()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//	fmt.Println(nr)
//
//	err = stream.Read()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	stream.Stop()
//	fmt.Println(time.Since(s))
//
//	///////////////////////////////////////////////
//
//	stream2, err := portaudio.OpenStream(
//		portaudio.StreamParameters{
//
//			Input:      portaudio.StreamDeviceParameters{Device: nil},
//			Output:     portaudio.StreamDeviceParameters{Device: deviceInfo, Channels: 1, Latency: 0},
//			Flags:      portaudio.NoFlag,
//			SampleRate: 44100,
//		},
//		buffer1,
//	)
//
//	err = stream2.Start()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//	err = stream2.Write()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//}
