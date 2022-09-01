package main

import (
	"Licenta/kafka"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"time"
)

const (
	interAppTopic = "messages"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		checkErr(err)

		interAppConsumer := kafka.NewKafkaConsumer(interAppTopic)
		checkErr(interAppConsumer.SetOffsetToNow())

		for {
			s := time.Now()
			message, err := interAppConsumer.Consume()
			checkErr(err)

			checkErr(ws.WriteMessage(websocket.BinaryMessage, message.Value))
			fmt.Println(time.Since(s))
		}
	})

	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
