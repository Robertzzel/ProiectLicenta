package main

import (
	"github.com/Shopify/sarama"
)

//const kafkaAddress = "localhost:9092"

type KafkaConsumer struct {
	consumer sarama.PartitionConsumer
	topic    string
}

func NewKafkaConsumer(topic string) (*KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//config.Offsets.ProcessingTimeout = 10 * time.Second

	// join to consumer group
	//cg, err := consumergroup.JoinConsumerGroup(consumerGroup, []string{topic}, []string{kafkaAddress}, config)
	//if err != nil {
	//	return nil, err
	//}

	consumer, err := sarama.NewConsumer([]string{kafkaAddress}, config)
	if err != nil {
		return nil, err
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		consumer: partitionConsumer,
		topic:    topic,
	}, err
}

func (cg *KafkaConsumer) consume() *sarama.ConsumerMessage {
	for {
		msg := <-cg.consumer.Messages()
		return msg
	}
}

//func main() {
//	c, err := NewKafkaConsumer("quickstart-events", "senz")
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	consume := c.consume()
//	if err != nil {
//		return
//	}
//
//	fmt.Print(string(consume.Value))
//
//	consume = c.consume()
//	if err != nil {
//		return
//	}
//
//	fmt.Print(string(consume.Value))
//}
