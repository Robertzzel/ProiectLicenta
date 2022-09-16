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

func processFiles(videoFileName, audioFileName string) (string, error) {
	filePattern := fmt.Sprintf("*out%d.mp4", time.Now().Unix())

	outputFile, err := os.CreateTemp("", filePattern)
	if err != nil {
		return "", err
	}

	if _, err := exec.Command("./CombineAndCompress", videoFileName, audioFileName, outputFile.Name(), "30").Output(); err != nil {
		return "", err
	}

	return outputFile.Name(), nil
}

func readSize(connection net.Conn) (int, error) {
	buffer := make([]byte, 10)
	_, err := io.LimitReader(connection, 10).Read(buffer)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(buffer))
}

func sendMessage(connection net.Conn, message []byte) error {
	if _, err := connection.Write([]byte(fmt.Sprintf("%010d", len(message)))); err != nil {
		return err
	}

	_, err := connection.Write(message)
	return err
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

	go func() {
		for {
			conn, err := listener.Accept()
			checkErr(err)

			go func(connection net.Conn) {
				defer connection.Close()

				for {
					size, err := readSize(connection)
					checkErr(err)

					message := make([]byte, size)
					_, err = connection.Read(message)
					checkErr(err)

					messageString := string(message)
					log.Println(messageString)

					if strings.HasSuffix(messageString, ".mkv") {
						videoFiles <- messageString
					} else if strings.HasSuffix(messageString, ".wav") {
						audioFiles <- messageString
					}
				}
			}(conn)
		}
	}()

	var routerConnection net.Conn = nil
	routerListener, err := net.Listen("unix", routerSocketName)
	checkErr(err)

	go func() {
		routerConnection, err = routerListener.Accept()
		checkErr(err)
	}()

	for {
		go func(videoFile, audioFile string) {
			fmt.Println("Primit", videoFile, audioFile, " la ", time.Now().Unix())
			fileName, err := processFiles(videoFile, audioFile)
			checkErr(err)

			if routerConnection != nil {
				checkErr(sendMessage(routerConnection, []byte(fileName)))
			}

			fmt.Println("Trimis", fileName, "la", time.Now().Unix(), "\n")
			checkErr(os.Remove(videoFile))
			checkErr(os.Remove(audioFile))
		}(<-videoFiles, <-audioFiles)
	}
}
