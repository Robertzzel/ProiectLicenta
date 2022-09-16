package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
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

	requestChannel := make(chan int, 2)
	responseChannel := make(chan int64, 2)

	go func() {
		<-requestChannel
		<-requestChannel

		currentTime := time.Now().Unix() + 1

		responseChannel <- currentTime
		responseChannel <- currentTime
	}()

	listener, err := net.Listen("unix", socketName)
	checkErr(err)
	defer listener.Close()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		conn, err := listener.Accept()
		checkErr(err)

		wg.Add(1)
		go func() {
			defer conn.Close()

			requestChannel <- 1
			_, err := conn.Write([]byte(fmt.Sprint(<-responseChannel)))
			checkErr(err)

			wg.Done()
		}()
	}

	wg.Wait()
}
