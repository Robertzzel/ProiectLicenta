package main

import (
	"Licenta/kafka"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
)

const (
	receivedImagesTopic = "rImages"
	receivedAudioTopic  = "rAudio"
)

func main() {
	receivedImagesProducer := kafka.NewImageKafkaProducer(receivedImagesTopic)
	receivedAudioProducer := kafka.NewImageKafkaProducer(receivedAudioTopic)

	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		return
	}
	defer conn.Close()

	fmt.Println("Conexiune stbil")
	sizeBuffer := make([]byte, 9)
	for {

		fmt.Println("ASteopt mesaj")

		_, err := conn.Read(sizeBuffer)
		if err != nil {
			fmt.Println(err)
			continue
		}

		size, err := strconv.Atoi(string(sizeBuffer))
		if err != nil {
			fmt.Println(err)
			continue
		}

		encodedMessage, err := io.ReadAll(io.LimitReader(conn, int64(size)))
		if err != nil {
			fmt.Println(err)
			continue
		}

		message := &kafka.InterAppMessage{}
		err = json.Unmarshal(encodedMessage, message)
		if err != nil {
			fmt.Println(err)
			continue
		}

		//fmt.Println(time.Now().Sub(message.Timestamp)-time.Since(s), time.Since(s))
		fmt.Println(len(message.Images), len(message.Audio))

		receivedAudioProducer.Publish(message.Audio)
		for _, image := range message.Images {
			receivedImagesProducer.Publish(image)
		}
	}
}
