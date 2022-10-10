package main

import (
	. "Licenta/SocketFunctions"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
)

const (
	videoSize          = time.Second
	syncSocketName     = "/tmp/sync.sock"
	composerSocketName = "/tmp/composer.sock"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func synchronise() (time.Time, error) {
	conn, err := net.Dial("unix", syncSocketName)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()

	message, err := ReceiveMessage(conn)
	if err != nil {
		return time.Time{}, err
	}

	timestamp, err := strconv.ParseInt(string(message), 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(timestamp, 0), nil
}

func main() {
	log.Println("Starting...")
	composerConnection, err := net.Dial("unix", composerSocketName)
	checkErr(err)

	videoRecorder, err := NewRecorder(24)
	checkErr(err)

	startTime, err := synchronise()
	checkErr(err)
	log.Println("SYNC: ", startTime.UnixMilli())

	videoRecorder.Start(startTime, videoSize)
	for {
		videoName := <-videoRecorder.VideoBuffer
		checkErr(SendMessage(composerConnection, []byte(videoName)))
		fmt.Println(videoName, "sent at ", time.Now().UnixMilli())
	}
}
