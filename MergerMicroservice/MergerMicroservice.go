package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"time"
)

const (
	socketName        = "/tmp/merger.sock"
	receivedVideosDir = "/tmp/receivedVideos"
	databaseSocket    = "/tmp/database.sock"
)

func getUIConnection() (net.Conn, error) {
	listener, err := net.Listen("unix", socketName)
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	return listener.Accept()
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

	if err := os.Mkdir(receivedVideosDir, os.ModePerm); err != nil {
		log.Println("Cannot create dir for received videos", err)
	}

	log.Println("Connecting to UI...")
	uiConnection, err := getUIConnection()
	if err != nil {
		log.Fatal("Cannot connect to UI", err)
	}
	defer uiConnection.Close()

	receivedVideos := make([]string, 0, 16)
	log.Println("Started...")
	for {
		video, err := ReceiveMessage(uiConnection)
		if err != nil {
			log.Println("Error while receiving videos ", err)
			break
		}

		file, err := os.CreateTemp(receivedVideosDir, "*.mp4")
		if err != nil {
			log.Println("Cannot create file for video", err)
			break
		}

		_, err = file.Write(video)
		if err != nil {
			log.Println("Cannot write received video in a file", err)
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
	connection, err := net.Dial("unix", databaseSocket)
	if err != nil {
		return
	}
	defer connection.Close()

	if err := SendMessage(connection, []byte(finalVideoFile)); err != nil {
		log.Fatal("Sending message to database microserv")
	}

	log.Println("Cleaning ", len(receivedVideos), " files..")
	for _, videoFile := range receivedVideos {
		if err = os.Remove(videoFile); err != nil {
			log.Println("Cannot remove file ", videoFile, " : ", err)
		}
	}
}
