package main

import (
	. "Licenta/SocketFunctions"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

const (
	socketName = "/tmp/sync.sock"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		checkErr(os.Remove(socketName))
	}

	listener, err := net.Listen("unix", socketName)
	checkErr(err)
	defer listener.Close()

	for {
		connections := make([]net.Conn, 0, 2)

		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			checkErr(err)

			connections = append(connections, conn)
		}

		currentTime := []byte(fmt.Sprintf("%010d", time.Now().Unix()+1))

		checkErr(SendMessage(connections[0], currentTime))
		checkErr(SendMessage(connections[1], currentTime))
	}
}
