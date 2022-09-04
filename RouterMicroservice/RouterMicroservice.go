package main

import (
	"Licenta/kafka"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"os"
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
		log.Println("Conectare")

		ws, err := upgrader.Upgrade(w, r, nil)
		checkErr(err)

		routerConsumer := kafka.NewKafkaConsumer(interAppTopic)
		checkErr(routerConsumer.SetOffsetToNow())

		for {
			message, err := routerConsumer.Consume()
			checkErr(err)

			fileName := string(message.Value)
			fileContents, err := os.ReadFile(fileName)
			checkErr(err)

			err = ws.WriteMessage(websocket.BinaryMessage, fileContents)
			if err != nil {
				break
			}

			checkErr(os.Remove(fileName))
		}

		checkErr(ws.Close())
	})

	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
