package main

import (
	"context"
	"database.microservice/Kafka"
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

func handleRequest(db *DatabaseManager, message *Kafka.CustomMessage, producer *Kafka.DatabaseProducer) {
	if !message.ValidateHeaders() {
		return
	}

	var headers = message.GetHeaders()
	var sendTopic = string(headers["topic"])
	var operation = string(headers["operation"])
	var sessionId, _ = strconv.Atoi(string(headers["sessionId"]))
	var partition, _ = strconv.Atoi(string(headers["partition"]))

	if operation != "LOGIN" && operation != "REGISTER" && operation != "ADD_VIDEO" && operation != "DELETE_SESSION" {
		name, hasUsername := headers["Name"]
		password, hasPassword := headers["Password"]

		if !hasUsername || !hasPassword {
			return
		}

		if userExists := db.UserExists(string(name), string(password)); userExists == false {
			if err := producer.Publish([]byte("permission denied"), []kafka.Header{{`status`, []byte("FAILED")}}, sendTopic, int32(partition)); err != nil {
				fmt.Println(err)
			}
		}
	}

	var response []byte
	var err error
	switch operation {
	case "LOGIN":
		response, err = db.HandleLogin(message.Value)
	case "REGISTER":
		response, err = db.HandleRegister(message.Value)
	case "ADD_VIDEO":
		response, err = db.HandleAddVideo(message.Value, sessionId)
	case "DELETE_VIDEO":
		response, err = db.DeleteVideo(message.Value)
	case "GET_CALL_BY_KEY":
		response, err = db.HandleGetCallByKeyAndPassword(message.Value)
	case "GET_VIDEOS_BY_USER":
		response, err = db.HandleGetVideosByUser(message.Value)
	case "DOWNLOAD_VIDEO_BY_ID":
		response, err = db.HandleDownloadVideoById(message.Value)
	case "CREATE_SESSION":
		response, err = db.HandleCreateSession(message.Value)
	case "DELETE_SESSION":
		response, err = db.HandleDeleteSession(sessionId)
	case "TOPICS":
		response, err = db.GetUserTopicsBySession(sessionId)
	case "CHANGE_PASSWORD":
		response, err = db.ChangeUserPassword(message.Value)
	default:
		err = errors.New("operation not permitted")
	}

	status := "OK"
	if err != nil {
		response = []byte("Error: " + err.Error())
		status = "FAILED"
		log.Println("$", string(response), "$")
	}
	if err = producer.Publish(response, []kafka.Header{{`status`, []byte(status)}}, sendTopic, int32(partition)); err != nil {
		fmt.Println(err)
	}
	producer.Flush(200)

	log.Println("Status:", status, "Opeartion:", operation, "Topic:", sendTopic, "Partition:", partition)
}

func main() {
	log.Println("Getting environment variables...")
	brokerAddress := "localhost:9092" // os.Getenv("BROKER_ADDRESS")
	databaseUser := "robert"          // os.Getenv("DATABASE_USER")
	databasePassword := "robert"      // os.Getenv("DATABASE_PASSWORD")
	databaseHost := "localhost"       //os.Getenv("DATABASE_HOST")
	databasePort := "3306"            // os.Getenv("DATABASE_PORT")
	databaseName := "licenta"         // os.Getenv("DATABASE_NAME")

	if brokerAddress == "" || databaseUser == "" || databasePassword == "" || databaseHost == "" || databasePort == "" || databaseName == "" {
		fmt.Println("Not all environment variables given")
		os.Exit(1)
	}

	log.Println("Getting database connection...")
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", databaseUser, databasePassword, databaseHost, databasePort, databaseName)

	db, err := NewDatabaseManager("mysql", connectionString)
	if err != nil {
		panic("cannot open the database " + err.Error())
	}
	defer func(db *DatabaseManager) {
		err := db.Close()
		if err != nil {
			log.Println("Error while closing database object ", err)
		}
	}(db)
	db.SetMaxOpenConns(10)

	log.Println("Migrating database ...")
	if err = db.MigrateDatabase(); err != nil {
		panic(err)
	}

	log.Println("Creating topic...")
	if err = Kafka.CreateTopic(brokerAddress, "DATABASE"); err != nil {
		panic(err)
	}

	log.Println("Getting a consumer connection...")
	consumer, err := Kafka.NewDatabaseConsumer(brokerAddress, topic)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = consumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	log.Println("Getting a producer connection...")
	producer, err := Kafka.NewDatabaseProducer(brokerAddress)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	ctx := NewContextCancelableBySignals()
	log.Println("Listening for messages...")
	for ctx.Err() == nil {
		message, err := consumer.ConsumeFullMessage(ctx)
		if err != nil {
			return
		}
		go handleRequest(db, message, producer)
	}
}
