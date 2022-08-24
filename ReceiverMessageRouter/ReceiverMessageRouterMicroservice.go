package main

import (
	"Licenta/kafka"
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"time"
)

const (
	interAppTopic = "messages"
)

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
		if err != nil {
			log.Println("Error ", err)
			return
		}

		_, m, err := ws.ReadMessage()
		if err != nil {
			log.Println("Error: ", err)
			return
		}
		log.Println("Msg type:", string(m))

		interAppConsumer := kafka.NewKafkaConsumer(interAppTopic)
		if err := interAppConsumer.Reader.SetOffsetAt(context.Background(), time.Now().Add(time.Hour)); err != nil {
			fmt.Println(err)
			return
		}

		for {
			message, err := interAppConsumer.Consume()
			if err != nil {
				log.Println(err)
				return
			}

			err = ws.WriteMessage(websocket.BinaryMessage, message.Value)
			if err != nil {
				log.Println("Eror", err)
			}
		}
	})

	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
