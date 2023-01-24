package main

import (
	"Licenta/Kafka"
	"fmt"
)

func main() {
	c := Kafka.NewConsumer("localhost:9092", "test")
	if err := c.Close(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Good")

}
