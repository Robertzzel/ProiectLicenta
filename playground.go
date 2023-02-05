package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"time"
)

func main() {
	topic := "testtopics"

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}
	res, err := a.DeleteTopics(
		context.Background(),
		[]string{topic},
	)
	time.Sleep(time.Second)
	res, err = a.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}},
	)
	if err != nil {
		panic(err)
	}
	println(res[0].String())

	time.Sleep(time.Second)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "sal2",
		"auto.offset.reset": "earliest",
	})

	if err = consumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: 1}}); err != nil {
		panic(err)
	}

mainloop:
	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			println(string(e.Value))
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			break mainloop
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
	consumer.Close()
}
