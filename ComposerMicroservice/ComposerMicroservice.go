package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	socketName       = "/tmp/composer.sock"
	routerSocketName = "/tmp/router.sock"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func receiveFiles(videoFiles, audioFiles chan string) {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		checkErr(os.Remove(socketName))
	}

	listener, err := net.Listen("unix", socketName)
	checkErr(err)
	defer listener.Close()

	for i := 0; i < 2; i++ {
		conn, err := listener.Accept()
		checkErr(err)

		go func(connection net.Conn) {
			defer connection.Close()

			for {
				message, err := ReceiveMessage(connection)
				if err != nil {
					return
				}

				messageString := string(message)
				if strings.HasSuffix(messageString, ".mkv") {
					videoFiles <- messageString
				} else if strings.HasSuffix(messageString, ".wav") {
					audioFiles <- messageString
				}
			}
		}(conn)
	}
}

func getRouterConnection() (net.Conn, error) {
	if _, err := os.Stat(routerSocketName); !errors.Is(err, os.ErrNotExist) {
		if err := os.Remove(routerSocketName); err != nil {
			return nil, err
		}
	}

	routerListener, err := net.Listen("unix", routerSocketName)
	if err != nil {
		return nil, err
	}

	return routerListener.Accept()
}

func getSyncedAudioAndVideo(videoChannel chan string, audioChannel chan string) (string, string, error) {
	videoFile := <-videoChannel
	audioFile := <-audioChannel

	videoTimestamp, err := strconv.Atoi(videoFile[len(videoFile)-14 : len(videoFile)-4])
	checkErr(err)
	audioTimestamp, err := strconv.Atoi(audioFile[len(audioFile)-14 : len(audioFile)-4])
	checkErr(err)

	timestampDifference := videoTimestamp - audioTimestamp
	if timestampDifference > 0 {
		log.Println("Desync ", "audio by ", timestampDifference)
		for i := 0; i < timestampDifference; i++ {
			audioFile = <-audioChannel
		}
	} else if timestampDifference < 0 {
		log.Println("Desync ", "video by ", timestampDifference)
		for i := 0; i < timestampDifference; i++ {
			videoFile = <-videoChannel
		}
	}

	return videoFile, audioFile, nil
}

func main() {
	videoFiles := make(chan string, 10)
	audioFiles := make(chan string, 10)

	go receiveFiles(videoFiles, audioFiles)

	routerConnection, err := getRouterConnection()
	checkErr(err)

	for {
		outputFile, err := os.CreateTemp("", "composed*.mp4")
		checkErr(err)
		checkErr(outputFile.Close())

		videoFile, audioFile, err := getSyncedAudioAndVideo(videoFiles, audioFiles)
		checkErr(err)

		go func(videoFile, audioFile, outputFile string) {
			s := time.Now()

			checkErr(exec.Command("./CombineAndCompress", videoFile, audioFile, outputFile, "0").Run())

			checkErr(SendMessage(routerConnection, []byte(outputFile)))
			fmt.Println(" video: ", outputFile, ", timestamp: ", videoFile[len(videoFile)-14:len(videoFile)-4], " at", time.Now().UnixMilli(), " (", time.Since(s), " )")

			os.Remove(videoFile)
			os.Remove(audioFile)
		}(videoFile, audioFile, outputFile.Name())

	}
}
