package main

import (
	kafka "github.com/Shopify/sarama"
	"log"
	"os"
)

const kafkaAddress = "localhost:9092"

type KafkaProducer struct {
	producer *kafka.SyncProducer
}

func NewKafkaProducer() (*KafkaProducer, error) {
	// setup sarama log to stdout
	kafka.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := kafka.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = kafka.WaitForAll
	config.Producer.Return.Successes = true

	// async producer
	//prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config)

	// sync producer
	prd, err := kafka.NewSyncProducer([]string{kafkaAddress}, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer: &prd,
	}, nil
}

func (kp *KafkaProducer) Publish(topic string, message string) error {
	msg := &kafka.ProducerMessage{
		Topic: topic,
		Value: kafka.StringEncoder(message),
	}

	_, _, err := (*kp.producer).SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

//func main() {
//	prd, err := NewKafkaProducer()
//	if err != nil {
//		fmt.Print(err)
//		return
//	}
//
//	for i := 1; i < 5; i++ {
//		err = prd.Publish("quickstart-events", fmt.Sprintf("%d", i))
//		if err != nil {
//			fmt.Println(err)
//			return
//		}
//	}
//
//}
