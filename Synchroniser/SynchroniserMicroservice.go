package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

const (
	socketName = "/tmp/sync.sock"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func getVideoAndAudioConnections(listener net.Listener) (net.Conn, net.Conn) {
	connections := [2]net.Conn{nil, nil}

	for i := 0; i < 2; i++ {
		conn, err := listener.Accept()
		checkErr(err)

		connections[i] = conn
	}

	return connections[0], connections[1]
}

func main() {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		checkErr(os.Remove(socketName))
	}

	listener, err := net.Listen("unix", socketName)
	checkErr(err)
	defer listener.Close()

	connection1, connection2 := getVideoAndAudioConnections(listener)
	defer connection1.Close()
	defer connection2.Close()

	currentTime := []byte(fmt.Sprintf("%010d", time.Now().Unix()+1))
	checkErr(SendMessage(connection1, currentTime))
	checkErr(SendMessage(connection2, currentTime))
}
