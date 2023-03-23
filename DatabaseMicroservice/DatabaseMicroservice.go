package main

import (
	"Licenta/Kafka"
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const (
	connectionString = "robert:robert@tcp(localhost:3306)/licenta?parseTime=true"
	topic            = "DATABASE"
)

func NewContextCancelableBySignals() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		cancel()
	}()

	return ctx
}

func handleRequest(db *gorm.DB, message []byte, headers []kafka.Header, producer *Kafka.DatabaseProducer) {
	var sendTopic, operation string
	var partition = 0
	var err error
	for _, header := range headers {
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
		response, err = HandleLogin(db, message)
	case "REGISTER":
		response, err = HandleRegister(db, message)
	case "ADD_VIDEO":
		var sessionId int
		for _, header := range headers {
			switch header.Key {
			case "sessionId":
				sessionIdString := string(header.Value)
				sessionId, err = strconv.Atoi(sessionIdString)
			}
		}
		if err != nil {
			break
		}

		response, err = HandleAddVideo(db, message, sessionId)
	case "GET_CALL_BY_KEY":
		response, err = HandleGetCallByKeyAndPassword(db, message)
	case "GET_VIDEOS_BY_USER":
		response, err = HandleGetVideoByUser(db, message)
	case "DOWNLOAD_VIDEO_BY_ID":
		response, err = HandleDownloadVideoById(db, message)
	case "DISCONNECT":
		response, err = HandleDisconnect(db, message)
	case "USERS_IN_SESSION":
		response, err = HandleUsersInSession(db, message)
	case "CREATE_SESSION":
		response, err = HandleCreateSession(db, message)
	case "DELETE_SESSION":
		response, err = HandleDeleteSession(db, message)
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

	consumer, err := Kafka.NewDatabaseConsumer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = consumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	producer, err := Kafka.NewDatabaseProducer(brokerAddress)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	ctx := NewContextCancelableBySignals()
	for {
		kafkaMessage, headers, err := consumer.ConsumeFullMessage(ctx)
		if err != nil {
			return
		}

		go handleRequest(db, kafkaMessage, headers, producer)
	}
}
