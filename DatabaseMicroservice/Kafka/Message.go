package Kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
)

type CustomMessage struct {
	*kafka.Message
}

func (message *CustomMessage) GetHeaders() map[string][]byte {
	headers := make(map[string][]byte)
	for _, m := range message.Headers {
		headers[m.Key] = m.Value
	}
	return headers
}

func (message *CustomMessage) ValidateHeaders() bool {
	headers := message.GetHeaders()
	_, hasTopic := headers["topic"]
	_, hasOperation := headers["operation"]
	partition, hasPartition := headers["partition"]
	sessionId, hasSessionId := headers["sessionId"]

	if hasSessionId {
		if _, err := strconv.Atoi(string(sessionId)); err != nil {
			return false
		}
	}

	if _, err := strconv.Atoi(string(partition)); err != nil {
		return false
	}

	return hasTopic && hasOperation && hasPartition
}
