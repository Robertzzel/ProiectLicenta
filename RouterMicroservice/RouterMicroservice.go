package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	routerSocketName = "/tmp/router.sock"
	port             = 8080
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", nil
}

func receiveMessage(connection net.Conn) ([]byte, error) {
	sizeBuffer := make([]byte, 10)
	if _, err := io.LimitReader(connection, 10).Read(sizeBuffer); err != nil {
		return nil, err
	}

	size, err := strconv.Atoi(string(sizeBuffer))
	if err != nil {
		return nil, err
	}

	messageBuffer := make([]byte, size)
	if _, err := connection.Read(messageBuffer); err != nil {
		return nil, err
	}

	return messageBuffer, nil
}

func sendMessage(connection net.Conn, message []byte) error {
	if _, err := connection.Write([]byte(fmt.Sprintf("%010d", len(message)))); err != nil {
		return err
	}

	_, err := connection.Write(message)
	return err
}

func main() {
	hostname, err := GetLocalIP()
	checkErr(err)

	log.Println("Ascult pentru clenti la", hostname, ":", port, " ...")
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, port))
	checkErr(err)

	clientConn, err := listener.Accept()
	checkErr(err)

	log.Println("Client conectat")
	connection, err := net.Dial("unix", routerSocketName)
	checkErr(err)

	for {
		fileName, err := receiveMessage(connection)
		fileContents, err := os.ReadFile(string(fileName))
		checkErr(err)

		checkErr(sendMessage(clientConn, fileContents))
		log.Println("Trimis", string(fileName), "la", time.Now().Unix())
	}

}
