package main

import (
	"Licenta/Kafka"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"gopkg.in/vansante/go-ffprobe.v2"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"regexp"
	"strconv"
	"time"
)

const (
	connectionString = "robert:robert@tcp(localhost:3306)/licenta?parseTime=true"
	topic            = "DATABASE"
)

func doesFileExist(fileName string) bool {
	_, err := os.Stat(fileName)

	return !os.IsNotExist(err)
}

func deleteSession(db *gorm.DB, sessionId uint) error {
	var foundSession Session
	if err := db.Where(&Session{Model: gorm.Model{ID: sessionId}}).First(&foundSession).Error; err != nil { // TODO VERIFICA SI DACA A FOST STEARSA
		return err
	}
	if err := db.Delete(&foundSession).Error; err != nil {
		return err
	}
	return nil
}

func WriteNewFile(data []byte) (string, error) {
	i := 0
	for {
		path := fmt.Sprintf("./video%d.mp4", i)
		if doesFileExist(path) {
			i++
			continue
		}
		if err := os.WriteFile(path, data, 0777); err != nil {
			return "", err
		}
		return path, nil
	}
}

func hash(password string) string {
	hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", hash)
}

func getUserFromMessage(message []byte) (User, error) {
	var jsonUser JsonUser
	if err := json.Unmarshal(message, &jsonUser); err != nil {
		return User{}, err
	}
	return jsonUser.ToUser(), nil
}

func handleLogin(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	name, nameExists := input["Name"]
	password, passwordExists := input["Password"]

	if !(nameExists && passwordExists) {
		return nil, errors.New("name, password and topic needed")
	}

	var user User
	if err := db.First(&user, "name = ? and password = ?", name, hash(password)).Error; err != nil {
		return nil, err
	}

	if err := db.Model(&user).Update("call_password", uuid.NewString()).Error; err != nil {
		return nil, err
	}

	if err := db.Model(&user).Update("session_id", nil).Error; err != nil {
		return nil, err
	}

	user.SessionId = nil
	return json.Marshal(&user)
}

func handleRegister(db *gorm.DB, message []byte) ([]byte, error) {
	user, err := getUserFromMessage(message)
	if err != nil {
		return nil, err
	}

	if len(user.Password) <= 4 || !regexp.MustCompile(`\d`).MatchString(user.Password) {
		return nil, errors.New("password mush have a number, a character and be at least the size of 4")
	}

	user.Password = hash(user.Password)
	user.CallKey = uuid.NewString() // PANICS

	if err = db.Create(&user).Error; err != nil {
		return nil, err
	}

	return []byte("Successfully created"), nil
}

func handleAddVideo(db *gorm.DB, message []byte, sessionId int) ([]byte, error) {
	var err error
	video := Video{}
	session := Session{}

	video.FilePath, err = WriteNewFile(message)
	if err != nil {
		return nil, err
	}

	videoDetails, err := ffprobe.ProbeURL(context.Background(), video.FilePath)
	if err != nil {
		return nil, err
	}

	video.DurationInSeconds = videoDetails.Format.DurationSeconds
	video.Size = videoDetails.Format.Size

	if err = db.Create(&video).Error; err != nil {
		return nil, err
	}

	if err = db.First(&session, "id = ?", sessionId).Error; err != nil {
		return nil, err
	}

	var associatedUsers []User
	if err = db.Where("session_id = ?", uint(sessionId)).Find(&associatedUsers).Error; err != nil {
		return nil, err
	}

	var foundUsers []User
	if err = db.Model(&session).Association("Users").Find(&foundUsers); err != nil {
		return nil, err
	}

	if err = db.Model(&video).Where("id = ?", video.ID).Association("Users").Append(&foundUsers); err != nil {
		return nil, err
	}

	return []byte("video added"), nil
}

func handleGetCallByKeyAndPassword(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	key, keyExists := input["Key"]
	password, passwordExists := input["Password"]
	callerIdString, hasCallerId := input["CallerId"]

	callerId, err := strconv.Atoi(callerIdString)
	if err != nil {
		return nil, err
	}

	if !keyExists || !passwordExists || !hasCallerId {
		return nil, errors.New("password AND key needed")
	}

	var sharerUser User
	if err = db.Where(&User{CallKey: key, CallPassword: password}).First(&sharerUser).Error; err != nil {
		return nil, err
	}

	var callerUser User
	if err = db.First(&callerUser, "id = ?", uint(callerId)).Error; err != nil {
		return nil, err
	}

	if sharerUser.SessionId == nil {
		return []byte("NOT ACTIVE"), nil
	}

	var session Session
	if err = db.Where(&Session{Model: gorm.Model{ID: *sharerUser.SessionId}}).First(&session).Error; err != nil {
		return nil, err
	}

	result, err := json.Marshal(map[string]string{"Topic": session.Topic})
	if err != nil {
		return nil, err
	}

	if err = db.Model(&callerUser).Update("session_id", session.ID).Error; err != nil {
		return nil, err
	}

	return result, nil
}

func handleGetVideoByUser(db *gorm.DB, message []byte) ([]byte, error) {
	user, err := getUserFromMessage(message)
	if err != nil {
		return nil, err
	}

	if err = db.First(&user, "id = ?", user.ID).Error; err != nil {
		return nil, err
	}

	var foundVideos []Video
	if err = db.Model(&user).Association("Videos").Find(&foundVideos); err != nil {
		return nil, err
	}

	result, err := json.Marshal(foundVideos)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func handleDownloadVideoById(db *gorm.DB, message []byte) ([]byte, error) {
	id, err := strconv.Atoi(string(message))
	if err != nil {
		return nil, err
	}

	video := Video{}
	if err = db.Where("id=?", uint(id)).First(&video).Error; err != nil {
		return nil, err
	}

	videoContents, err := os.ReadFile(video.FilePath)
	if err != nil {
		return nil, err
	}

	return videoContents, nil
}

func handleDisconnect(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	idString, idExists := input["ID"]
	if !idExists {
		return nil, errors.New("id not given")
	}

	id, err := strconv.Atoi(idString)
	if err != nil {
		return nil, err
	}

	var user User
	if err = db.First(&user, "id = ?", uint(id)).Error; err != nil {
		return nil, err
	}

	if user.SessionId != nil {
		if err = deleteSession(db, *user.SessionId); err != nil {
			return nil, err
		}
	}
	return []byte("success"), nil
}

func handleUsersInSession(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	sessionIdString, idExists := input["ID"]
	if !idExists {
		return nil, errors.New("sessionId not given")
	}

	sessionId, err := strconv.Atoi(sessionIdString)
	if err != nil {
		return nil, err
	}

	var session Session
	if err = db.First(&session, "id = ?", sessionId).Error; err != nil {
		return nil, err
	}

	var associatedUsers []User
	if err = db.Where("session_id = ?", uint(sessionId)).Find(&associatedUsers).Error; err != nil {
		return nil, err
	}

	return []byte(strconv.Itoa(len(associatedUsers))), nil
}

func handleCreateSession(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	topic, hasTopic := input["Topic"]
	userIdString, hasUserId := input["UserID"]

	if !(hasTopic && hasUserId) {
		return nil, errors.New("not enough topics")
	}

	userId, err := strconv.Atoi(userIdString)
	if err != nil {
		return nil, err
	}

	session := Session{Topic: topic}
	if err = db.Create(&session).Error; err != nil {
		return nil, err
	}

	var user User
	if err = db.First(&user, "id = ?", uint(userId)).Error; err != nil {
		return nil, err
	}

	if err = db.Model(&session).Association("Users").Append(&user); err != nil {
		return nil, err
	}

	return []byte(strconv.Itoa(int(session.ID))), nil
}

func handleDeleteSession(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	sessionIdString, hasSessionId := input["SessionId"]
	userIdString, hasUserId := input["UserId"]
	if !(hasSessionId && hasUserId) {
		return nil, errors.New("not id given")
	}

	sessionId, err := strconv.Atoi(sessionIdString)
	if err != nil {
		return nil, err
	}

	userId, err := strconv.Atoi(userIdString)
	if err != nil {
		return nil, err
	}

	var user User
	if err = db.First(&user, "id = ?", uint(userId)).Error; err != nil {
		return nil, err
	}

	if err = db.Model(&user).Update("session_id", nil).Error; err != nil {
		return nil, err
	} // TODO reseteaza session id urile tuturor iuserilor din conversatie

	var session Session
	if err = db.First(&session, "id = ?", sessionId).Error; err != nil {
		return nil, err
	}

	if err = db.Delete(&session).Error; err != nil {
		return nil, err
	}

	return []byte("success"), nil
}

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
		response, err = handleLogin(db, message.Message)
	case "REGISTER":
		response, err = handleRegister(db, message.Message)
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

		response, err = handleAddVideo(db, message.Message, sessionId)
	case "GET_CALL_BY_KEY":
		response, err = handleGetCallByKeyAndPassword(db, message.Message)
	case "GET_VIDEOS_BY_USER":
		response, err = handleGetVideoByUser(db, message.Message)
	case "DOWNLOAD_VIDEO_BY_ID":
		response, err = handleDownloadVideoById(db, message.Message)
	case "DISCONNECT":
		response, err = handleDisconnect(db, message.Message)
	case "USERS_IN_SESSION":
		response, err = handleUsersInSession(db, message.Message)
	case "CREATE_SESSION":
		response, err = handleCreateSession(db, message.Message)
	case "DELETE_SESSION":
		response, err = handleDeleteSession(db, message.Message)
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
