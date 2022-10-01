package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const (
	socketName        = "/tmp/merger.sock"
	receivedVideosDir = "/tmp/receivedVideos"
)

func getUIConnection() (net.Conn, error) {
	listener, err := net.Listen("unix", socketName)
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	return listener.Accept()
}

func receiveMessage(connection net.Conn) ([]byte, error) {
	sizeBuffer := make([]byte, 10)
	if _, err := io.LimitReader(connection, 10).Read(sizeBuffer); err != nil {
		return nil, err
	}

	size, err := strconv.Atoi(string(sizeBuffer))
	if err != nil {
		return nil, err
	}

	messageBuffer := make([]byte, size)
	if _, err := connection.Read(messageBuffer); err != nil {
		return nil, err
	}

	return messageBuffer, nil
}

func createFileWithVideos(videos []string) (string, error) {
	txtFile, err := os.CreateTemp(receivedVideosDir, "*.txt")
	if err != nil {
		return "", err
	}

	text := ""
	for _, videoFile := range videos {
		text += "file " + videoFile + "\n"
	}

	written, err := txtFile.WriteString(text)
	if err != nil {
		return "", err
	}

	for written != len(text) {
		notWrittenText := text[written:]
		written, err = txtFile.WriteString(notWrittenText)
		if err != nil {
			return "", err
		}
	}

	txtFile.Close()
	return txtFile.Name(), nil
}

func main() {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		if err := os.Remove(socketName); err != nil {
			log.Fatal("Cannot delete last unix socket", err)
		}
	}

	receivedVideos := make([]string, 0, 16)
	if err := os.Mkdir(receivedVideosDir, os.ModePerm); err != nil {
		log.Println("Cannot create dir for received videos", err)
	}

	log.Println("Connecting to UI...")
	uiConnection, err := getUIConnection()
	if err != nil {
		log.Fatal("Cannot connect to UI", err)
	}
	defer uiConnection.Close()

	log.Println("Started...")
	for {
		video, err := receiveMessage(uiConnection)
		if err != nil {
			log.Println("Error while receiving videos", err)
			break
		}

		file, err := os.CreateTemp(receivedVideosDir, "*.mp4")
		if err != nil {
			log.Println("Cannot create file for video", err)
			break
		}

		_, err = file.Write(video)
		if err != nil {
			log.Println("Cannot write video file", err)
			break
		}

		file.Close()
		receivedVideos = append(receivedVideos, file.Name())
	}

	txtFileName, err := createFileWithVideos(receivedVideos)
	if err != nil {
		log.Fatal("Error while creating txt file with all videos ", err)
	}

	finalVideoFile := fmt.Sprintf("%d.mp4", time.Now().Unix())
	if _, err := exec.Command("./VideoConcatter", txtFileName, finalVideoFile).Output(); err != nil {
		log.Fatal("Error while concatting received videos ", err)
	}

	log.Println("Created", finalVideoFile)

	for _, videoFile := range receivedVideos {
		os.Remove(videoFile)
	}
}
