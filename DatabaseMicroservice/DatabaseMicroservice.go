package main

import (
	"Licenta/Kafka"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"strconv"
	"time"
)

const (
	connectionString = "robert:robert@tcp(localhost:3306)/licenta?parseTime=true"
	topic            = "DATABASE"
)

func handleRequest(db *gorm.DB, message *Kafka.ConsumerMessage, producer *Kafka.DatabaseProducer) {
	var sendTopic, operation string
	var partition = 0
	var err error
	for _, header := range message.Headers {
		switch header.Key {
		case "topic":
			sendTopic = string(header.Value)
		case "operation":
			operation = string(header.Value)
		case "partition":
			partition, err = strconv.Atoi(string(header.Value))
		}
	}

	var response []byte
	switch operation {
	case "LOGIN":
		response, err = HandleLogin(db, message.Message)
	case "REGISTER":
		response, err = HandleRegister(db, message.Message)
	case "ADD_VIDEO":
		var sessionId int
		for _, header := range message.Headers {
			switch header.Key {
			case "sessionId":
				sessionIdString := string(header.Value)
				sessionId, err = strconv.Atoi(sessionIdString)
			}
		}
		if err != nil {
			break
		}

		response, err = HandleAddVideo(db, message.Message, sessionId)
	case "GET_CALL_BY_KEY":
		response, err = HandleGetCallByKeyAndPassword(db, message.Message)
	case "GET_VIDEOS_BY_USER":
		response, err = HandleGetVideoByUser(db, message.Message)
	case "DOWNLOAD_VIDEO_BY_ID":
		response, err = HandleDownloadVideoById(db, message.Message)
	case "DISCONNECT":
		response, err = HandleDisconnect(db, message.Message)
	case "USERS_IN_SESSION":
		response, err = HandleUsersInSession(db, message.Message)
	case "CREATE_SESSION":
		response, err = HandleCreateSession(db, message.Message)
	case "DELETE_SESSION":
		response, err = HandleDeleteSession(db, message.Message)
	default:
		err = errors.New("operation not permitted")
	}
	status := "OK"
	if err != nil {
		response = []byte(err.Error())
		status = "FAILED"
	}
	if err = producer.Publish(response, []kafka.Header{{`status`, []byte(status)}}, sendTopic, int32(partition)); err != nil {
		fmt.Println(err)
	}
	producer.Flush(100)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("No broker address given")
		return
	}
	brokerAddress := os.Args[1]

	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})
	if err != nil {
		panic("cannot open the database")
	}

	if err = db.AutoMigrate(&Session{}, &User{}, &Video{}); err != nil {
		panic(err)
	}

	consumer := Kafka.NewConsumer(brokerAddress, topic)
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	if err = consumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	producer, err := Kafka.NewDatabaseProducer(brokerAddress)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for {
		kafkaMessage, err := consumer.Consume(time.Second * 2)
		if err != nil {
			continue
		}
		fmt.Print("Message ")

		go handleRequest(db, kafkaMessage, producer)
	}
}
