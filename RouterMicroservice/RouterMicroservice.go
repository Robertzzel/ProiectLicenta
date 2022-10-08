package main

import (
	. "Licenta/SocketFunctions"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

const (
	routerSocketName    = "/tmp/router.sock"
	port                = 8080
	inputExecutorSocket = "/tmp/inputExecutor.sock"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func GetLocalIP() (string, error) {
	interfaceAddresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range interfaceAddresses {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", nil
}

func main() {
	hostname, err := GetLocalIP()
	checkErr(err)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, port))
	checkErr(err)

	log.Println("Listening at", hostname, ":", port, " ...")
	clientConn, err := listener.Accept()
	checkErr(err)

	log.Println("Client connected")
	connection, err := net.Dial("unix", routerSocketName)
	checkErr(err)

	go func() {
		conn, err := net.Dial("unix", inputExecutorSocket)
		checkErr(err)

		for {
			inputReceived, err := ReceiveMessage(clientConn)
			checkErr(err)
			checkErr(SendMessage(conn, inputReceived))
		}
	}()

	for {
		fileName, err := ReceiveMessage(connection)
		fileContents, err := os.ReadFile(string(fileName))
		checkErr(err)

		checkErr(SendMessage(clientConn, fileContents))
		log.Println("Sent", string(fileName), "at", time.Now().Unix())
	}

}
