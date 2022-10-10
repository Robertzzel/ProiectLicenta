package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
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

func receiveFiles(listener net.Listener, videoFiles, audioFiles chan string) {
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

func processFiles(videoFileName, audioFileName string) (string, error) {
	filePattern := fmt.Sprintf("*out%s.mp4", strings.Split(strings.Split(videoFileName, "/")[1], ".")[0])

	outputFile, err := os.CreateTemp("", filePattern)
	if err != nil {
		return "", err
	}

	if _, err := exec.Command("./CombineAndCompress", videoFileName, audioFileName, outputFile.Name(), "35").Output(); err != nil {
		return "", err
	}

	return outputFile.Name(), nil
}

func main() {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		checkErr(os.Remove(socketName))
	}

	if _, err := os.Stat(routerSocketName); !errors.Is(err, os.ErrNotExist) {
		checkErr(os.Remove(routerSocketName))
	}

	videoFiles := make(chan string, 10)
	audioFiles := make(chan string, 10)

	listener, err := net.Listen("unix", socketName)
	checkErr(err)
	defer listener.Close()

	go receiveFiles(listener, videoFiles, audioFiles)

	var routerConnection net.Conn = nil
	routerListener, err := net.Listen("unix", routerSocketName)
	checkErr(err)

	go func() {
		routerConnection, err = routerListener.Accept()
		checkErr(err)
	}()

	for {
		go func(videoFile, audioFile string) {
			defer func() {
				os.Remove(videoFile)
				os.Remove(audioFile)
			}()

			if videoFile[len(videoFile)-14:len(videoFile)-4] != audioFile[len(audioFile)-14:len(audioFile)-4] {
				log.Println("DESYNC ", videoFile, " ", audioFile)
			}

			s := time.Now()
			fileName, err := processFiles(videoFile, audioFile)
			checkErr(err)
			fmt.Print(" ", time.Since(s))

			if routerConnection != nil {
				checkErr(SendMessage(routerConnection, []byte(fileName)))
				fmt.Println(" Sent", fileName, "at", time.Now().Unix())
			} else {
				fmt.Println(" Not sent at ", time.Now().Unix())
			}
		}(<-videoFiles, <-audioFiles)
	}
}
