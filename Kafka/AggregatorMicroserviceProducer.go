package Kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type AggregatorMicroserviceProducer struct {
	*ProducerWrapper
	topic          string
	deliverChannel chan kafka.Event
}

func (producer *AggregatorMicroserviceProducer) PublishVideoStart(message []byte, headers []kafka.Header) error {
	return producer.ProducerWrapper.Publish(message, headers, producer.topic, VideoMicroservicePartition, producer.deliverChannel)
}

func (producer *AggregatorMicroserviceProducer) PublishAudioStart(message []byte, headers []kafka.Header) error {
	return producer.ProducerWrapper.Publish(message, headers, producer.topic, AudioMicroservicePartition, producer.deliverChannel)
}

func (producer *AggregatorMicroserviceProducer) PublishMerger(message []byte, headers []kafka.Header) error {
	return producer.ProducerWrapper.Publish(message, headers, producer.topic, MergerMicroservicePartition, producer.deliverChannel)
}

func (producer *AggregatorMicroserviceProducer) PublishClient(message []byte, headers []kafka.Header) error {
	return producer.ProducerWrapper.Publish(message, headers, producer.topic, ClientPartition, producer.deliverChannel)
}

func NewAggregatorMicroserviceProducer(brokerAddress, topic string) (*AggregatorMicroserviceProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"client.id":         "-",
		"acks":              "all",
	})
	if err != nil {
		return nil, err
	}

	return &AggregatorMicroserviceProducer{topic: topic, ProducerWrapper: &ProducerWrapper{p}, deliverChannel: make(chan kafka.Event, 5)}, nil
}
