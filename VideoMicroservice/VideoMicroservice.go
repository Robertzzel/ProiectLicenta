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
		panic(err)
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

	videoRecorder, err := NewRecorder(30)
	checkErr(err)
	videoRecorder.Start()

	startTime, err := synchronise()
	checkErr(err)
	fmt.Println("SYNC: ", startTime.Unix())

	for iteration := 0; ; iteration++ {
		partStartTime := startTime.Add(time.Duration(int64(videoSize) * int64(iteration)))
		fileName := "videos/" + fmt.Sprint(partStartTime.Unix()) + ".mkv"

		checkErr(videoRecorder.CreateFile(fileName, partStartTime, videoSize))
		checkErr(SendMessage(composerConnection, []byte(fileName)))

		fmt.Println("video", fileName, "sent at ", time.Now().Unix())
	}
}
