package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
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
	filePattern := fmt.Sprintf("*out%s.mp4", videoFileName[8:19])

	outputFile, err := os.CreateTemp("", filePattern)
	if err != nil {
		return "", err
	}

	if _, err := exec.Command("./CombineAndCompress", videoFileName, audioFileName, outputFile.Name(), "0").Output(); err != nil {
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
			defer os.Remove(videoFile)
			defer os.Remove(audioFile)

			s := time.Now()
			fileName, err := processFiles(videoFile, audioFile)
			fmt.Println(time.Since(s))
			checkErr(err)

			if routerConnection != nil {
				checkErr(SendMessage(routerConnection, []byte(fileName)))
			}

			fmt.Println("Sent", fileName, "at", time.Now().Unix())
		}(<-videoFiles, <-audioFiles)
	}
}
