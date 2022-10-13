package main

import (
	"Licenta/Kafka"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"
)

const (
	receivedVideosDir = "/tmp/receivedVideos"
	UiTopic           = "UI"
	MergerTopic       = "Merger"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func createFileWithVideoNames(videos []string) (string, error) {
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
	log.Println("Started...")

	checkErr(Kafka.CreateTopic(MergerTopic))
	defer Kafka.DeleteTopic(MergerTopic)

	uiConsumer := Kafka.NewConsumer(UiTopic)
	defer uiConsumer.Close()

	mergerPublisher := Kafka.NewProducer(MergerTopic)
	defer mergerPublisher.Close()

	os.Mkdir(receivedVideosDir, os.ModePerm)

	receivedVideos := make([]string, 0, 16)
	for {
		video, err := uiConsumer.Consume()
		if err != nil {
			log.Println("Error while receiving videos ", err)
			break
		}

		if string(video.Value) == "quit" {
			break
		}

		file, err := os.CreateTemp(receivedVideosDir, "*.mp4")
		if err != nil {
			log.Println("Cannot create file for video", err)
			break
		}

		_, err = file.Write(video.Value)
		if err != nil {
			log.Println("Cannot write received video in a file", err)
			break
		}

		file.Close()
		receivedVideos = append(receivedVideos, file.Name())
	}

	txtFileName, err := createFileWithVideoNames(receivedVideos)
	checkErr(err)

	finalVideoFile := fmt.Sprintf("%d.mp4", time.Now().Unix())
	checkErr(exec.Command("./VideoConcatter", txtFileName, finalVideoFile).Run())
	log.Println("Created", finalVideoFile)

	databaseMessage := fmt.Sprintf("insert;%s,%d", finalVideoFile, time.Now().Unix())
	checkErr(mergerPublisher.Publish([]byte(databaseMessage)))

	// Cleaning
	log.Println("Cleaning ", len(receivedVideos), " files..")
	for _, videoFile := range receivedVideos {
		if err = os.Remove(videoFile); err != nil {
			log.Println("Cannot remove file ", videoFile, " : ", err)
		}
	}
}
