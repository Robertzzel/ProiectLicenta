package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

type AudioVideoPair struct {
	Video string
	Audio string
}

func (avp *AudioVideoPair) Delete() error {
	if err := os.Remove(avp.Video); err != nil {
		return err
	}

	if err := os.Remove(avp.Audio); err != nil {
		return err
	}

	return nil
}

func (avp *AudioVideoPair) GetVideoTimestamp() (int, error) {
	return strconv.Atoi(avp.Video[len(avp.Video)-17 : len(avp.Video)-4])
}

func (avp *AudioVideoPair) GetAudioTimestamp() (int, error) {
	return strconv.Atoi(avp.Audio[len(avp.Audio)-17 : len(avp.Audio)-4])
}

func (avp *AudioVideoPair) CombineAndCompress(compressFactor int, output string, buffer *bytes.Buffer) error {
	//errBuffer := bytes.Buffer{}
	cmd := exec.Command("./CombineAndCompress", avp.Video, avp.Audio, fmt.Sprint(compressFactor), output)
	cmd.Stdout = buffer
	return cmd.Run()
	//cmd.Stderr = &errBuffer
	//if err := cmd.Run(); err != nil {
	//	return errors.New(errBuffer.String())
	//}
	//return nil
}
