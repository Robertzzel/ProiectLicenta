package main

import (
	"context"
	"database.microservice/Kafka"
	"database/sql"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const (
	topic = "DATABASE"
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

func handleRequest(db *sql.DB, message *kafka.Message, producer *Kafka.DatabaseProducer) {
	var sendTopic, operation string
	var sessionId int
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
		case "sessionId":
			sessionId, err = strconv.Atoi(string(header.Value))
			if err != nil {
				if err = producer.Publish([]byte("wrong session id"), []kafka.Header{{`status`, []byte("FAILED")}}, sendTopic, int32(partition)); err != nil {
					log.Println(err)
				}
			}
		}
	}

	var response []byte
	switch operation {
	case "LOGIN":
		response, err = HandleLogin(db, message.Value)
	case "REGISTER":
		response, err = HandleRegister(db, message.Value)
	case "ADD_VIDEO":
		response, err = HandleAddVideo(db, message.Value, sessionId)
	case "GET_CALL_BY_KEY":
		response, err = HandleGetCallByKeyAndPassword(db, message.Value)
	case "GET_VIDEOS_BY_USER":
		response, err = HandleGetVideosByUser(db, message.Value)
	case "DOWNLOAD_VIDEO_BY_ID":
		response, err = HandleDownloadVideoById(db, message.Value)
	case "CREATE_SESSION":
		response, err = HandleCreateSession(db, message.Value)
	case "DELETE_SESSION":
		response, err = HandleDeleteSession(db, sessionId)
	default:
		err = errors.New("operation not permitted")
	}
	status := "OK"
	if err != nil {
		response = []byte(err.Error())
		status = "FAILED"
		log.Println("$", err, "$")
	}
	if err = producer.Publish(response, []kafka.Header{{`status`, []byte(status)}}, sendTopic, int32(partition)); err != nil {
		fmt.Println(err)
	}
	producer.Flush(200)

	log.Println("Status:", status, "Opeartion:", operation, "Topic:", sendTopic, "Partition:", partition)
}

func MigrateDatabase(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS Session(
    Id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Topic varchar(255) NOT NULL)`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS User(
    	Id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		Name varchar(255) NOT NULL,
		Password varchar(255),
		CallKey varchar(255) NOT NULL,
		CallPassword varchar(255) NOT NULL,
		SessionId int,
    	FOREIGN KEY (SessionId) REFERENCES Session(Id),
    	UNIQUE (Name),
    	UNIQUE (CallKey)
    )`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS Video(
    Id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    FilePath varchar(255) NOT NULL,
    Duration DOUBLE(255, 2),
    Size varchar(255),
    UNIQUE (FilePath)
)`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS UserVideo(
    UserId int,
    VideoId int,
    FOREIGN KEY (UserId) REFERENCES User(Id),
    FOREIGN KEY (VideoId) REFERENCES Video(Id)
)`)
	return err
}

func main() {
	brokerAddress := os.Getenv("BROKER_ADDRESS")
	databaseUser := os.Getenv("DATABASE_USER")
	databasePassword := os.Getenv("DATABASE_PASSWORD")
	databaseHost := os.Getenv("DATABASE_HOST")
	databasePort := os.Getenv("DATABASE_PORT")
	databaseName := os.Getenv("DATABASE_NAME")

	if brokerAddress == "" || databaseUser == "" || databasePassword == "" || databaseHost == "" || databasePort == "" || databaseName == "" {
		fmt.Println("Not all environment variables given")
		os.Exit(1)
	}

	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", databaseUser, databasePassword, databaseHost, databasePort, databaseName)

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		panic("cannot open the database " + err.Error())
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Println("Error while closing database object ", err)
		}
	}(db)
	db.SetMaxOpenConns(10)

	if err = MigrateDatabase(db); err != nil {
		panic(err)
	}

	if err = Kafka.CreateTopic(brokerAddress, "DATABASE"); err != nil {
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
	for ctx.Err() == nil {
		message, err := consumer.ConsumeFullMessage(ctx)
		if err != nil {
			return
		}
		//marshal, _ := json.Marshal(map[string]string{"Name": "robert", "Password": "robert"})
		//message := &kafka.Message{Headers: []kafka.Header{{Key: "sessionId", Value: []byte("1")}, {Key: "operation", Value: []byte("LOGIN")}},
		//	Value: marshal}

		log.Println("Message")
		go handleRequest(db, message, producer)
	}
}
