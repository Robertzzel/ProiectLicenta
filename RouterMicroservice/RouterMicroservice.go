package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	routerSocketName = "/tmp/router.sock"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	connection, err := net.Dial("unix", routerSocketName)
	if err != nil {
		return
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Conectare")

		ws, err := upgrader.Upgrade(w, r, nil)
		checkErr(err)

		for {

			fileName, err := receiveMessage(connection)
			fileContents, err := os.ReadFile(string(fileName))
			checkErr(err)

			err = ws.WriteMessage(websocket.BinaryMessage, fileContents)
			if err != nil {
				break
			}
			fmt.Println(string(fileName), "at ", time.Now().Unix())

			checkErr(os.Remove(string(fileName)))
		}

		checkErr(ws.Close())
	})

	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
