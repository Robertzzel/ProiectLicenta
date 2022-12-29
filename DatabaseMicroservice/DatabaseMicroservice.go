package main

import (
	"Licenta/Kafka"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"regexp"
	"time"
)

const (
	connectionString = "robert:robert@tcp(localhost:3306)/licenta?parseTime=true"
	topic            = "DATABASE"
)

func doesFileExist(fileName string) bool {
	_, err := os.Stat(fileName)

	// check if error is "file not exists"
	return !os.IsNotExist(err)
}

func addUser() *Kafka.ConsumerMessage {
	return &Kafka.ConsumerMessage{
		Headers: []Kafka.Header{
			{"topic", []byte("UI")},
			{"operation", []byte("CREATE")},
			{"table", []byte("users")},
			{"input", []byte(`{"Name": "admin","Password": "admin"}`)},
		},
	}
}

func addVideo() *Kafka.ConsumerMessage {
	return &Kafka.ConsumerMessage{
		Headers: []Kafka.Header{
			{"topic", []byte("none")},
			{"operation", []byte("CREATE")},
			{"table", []byte("videos")},
			{"input", []byte(`{"Users": [{"ID": 1}]}`)},
		},
	}
}

func getVideosByUser() *Kafka.ConsumerMessage {
	return &Kafka.ConsumerMessage{
		Headers: []Kafka.Header{
			{"topic", []byte("none")},
			{"operation", []byte("READ_VIDEOS")},
			{"table", []byte("users")},
			{"input", []byte(`{"ID": 1}`)},
		},
	}
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

func handleUserRequest(db *gorm.DB, operation string, input []byte) (*Kafka.ProducerMessage, error) {
	var inputUser JsonUser
	if err := json.Unmarshal(input, &inputUser); err != nil {
		return nil, err
	}
	user := inputUser.ToUser()
	user.Password = hash(user.Password)

	switch operation {
	case "CREATE":
		if err := db.Create(&user).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error()), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, nil
		}
	case "READ": // check
		if err := db.Where("name = ? and password = ?", user.Name, user.Password).First(&user).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error()), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, nil
		}
		return &Kafka.ProducerMessage{Message: []byte(fmt.Sprint(user.ID)), Headers: []Kafka.Header{{Key: "status", Value: []byte("OK")}}}, nil
	case "READ_VIDEOS":
		var foundVideos []Video

		if err := db.Table("users").Where("id = ?", user.ID).First(&user).Error; err != nil { //.Association("Videos").Find(&foundVideos)
			return &Kafka.ProducerMessage{Message: []byte(err.Error()), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, nil
		}
		if err := db.Model(&user).Association("Videos").Find(&foundVideos); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error()), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, nil
		}

		result, err := json.Marshal(foundVideos)
		if err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error()), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, err
		}
		return &Kafka.ProducerMessage{Message: result, Headers: []Kafka.Header{{Key: "status", Value: []byte("OK")}}}, nil
	case "DELETE":
		if err := db.Delete(&user); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(db.Error.Error())}, nil
		}
	case "UPDATE":
		if err := db.Where("name = ?", user.Name).Update("password", user.Password); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error.Error())}, nil
		}
	}

	return &Kafka.ProducerMessage{Message: []byte("OK"), Headers: []Kafka.Header{{Key: "status", Value: []byte("OK")}}}, nil
}

func getUserFromMessage(message []byte) (User, error) {
	var jsonUser JsonUser
	if err := json.Unmarshal(message, &jsonUser); err != nil {
		return User{}, err
	}
	return jsonUser.ToUser(), nil
}

func handleVideoRequest(db *gorm.DB, operation string, input []byte, data []byte) (*Kafka.ProducerMessage, error) {
	var inputVideo JsonVideo
	if err := json.Unmarshal(input, &inputVideo); err != nil {
		return nil, err
	}
	video := inputVideo.ToVideo()
	users := video.Users
	video.Users = nil

	switch operation {
	case "CREATE":
		// create file
		var err error
		video.FilePath, err = WriteNewFile(data)
		if err != nil {
			return &Kafka.ProducerMessage{Message: []byte("FAILED")}, nil
		}

		if db.Create(&video).Error != nil {
			return &Kafka.ProducerMessage{Message: []byte(db.Error.Error())}, nil
		}

		foundUsers := make([]*User, 0)
		for _, user := range users {
			if db.Where("id = ?", user.ID).First(user).Error == nil {
				foundUsers = append(foundUsers, user)
			}
		}

		if err := db.Model(&video).Where("id = ?", video.ID).Association("Users").Append(&foundUsers); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error())}, nil
		}
	case "READ": // check
		video = Video{Model: gorm.Model{ID: inputVideo.ID}}
		if err := db.Where("id=?", video.ID).First(&video).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error())}, nil
		}
		videoContents, err := os.ReadFile(video.FilePath)
		if err != nil {
			return &Kafka.ProducerMessage{Message: []byte(""), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, err
		}
		return &Kafka.ProducerMessage{Message: videoContents, Headers: []Kafka.Header{{Key: "status", Value: []byte("OK")}}}, nil
	case "DELETE":
		if err := db.Delete(&video).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error())}, nil
		}
	}
	return &Kafka.ProducerMessage{Message: []byte("OK")}, nil
}

func handleLogin(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	name, nameExists := input["Name"]
	password, passwordExists := input["Password"]
	aggregatorTopic, aggregatorTopicExists := input["AggregatorTopic"]
	inputsTopic, inputsTopicExists := input["InputsTopic"]

	if !nameExists || !passwordExists || !aggregatorTopicExists || !inputsTopicExists {
		return nil, errors.New("name, password and topic needed")
	}

	var user User
	if err := db.First(&user, "name = ? and password = ?", name, hash(password)).Error; err != nil {
		return nil, err
	}

	if user.SessionId != 0 {
		var foundSession Session
		_ = db.Where(&Session{Model: gorm.Model{ID: user.SessionId}}).First(&foundSession).Error != nil &&
			db.Delete(&foundSession).Error != nil // TODO DO SOMETHING WITH IS VALUE
	}

	var session = Session{TopicAggregator: aggregatorTopic, TopicInputs: inputsTopic}
	if err := db.Create(&session).Error; err != nil {
		return nil, err
	}

	if err := db.Model(&user).Update("call_password", uuid.NewString()[:4]).Error; err != nil {
		return nil, err
	}

	if err := db.Model(&user).Update("session_id", session.ID).Error; err != nil {
		return nil, err
	}

	if err := db.Model(&session).Association("Users").Append(&user); err != nil {
		return nil, err
	}

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

func handleAddVideo(db *gorm.DB, message []byte) ([]byte, error) {
	var jsonVideo *JsonVideo
	var err error
	if err = json.Unmarshal(message, jsonVideo); err != nil {
		return nil, err
	}
	video := jsonVideo.ToVideo()
	video.FilePath, err = WriteNewFile(jsonVideo.Content)
	if err != nil {
		return nil, err
	}

	if err = db.Create(&video).Error; err != nil {
		return nil, err
	}

	foundUsers := make([]*User, 0)
	for _, user := range video.Users {
		if db.Where("id = ?", user.ID).First(user).Error == nil {
			foundUsers = append(foundUsers, user)
		}
	}

	if err = db.Model(&video).Where("id = ?", video.ID).Association("Users").Append(&foundUsers); err != nil {
		return nil, err
	}

	return []byte("video added"), nil
}

func handleIsUserActive(db *gorm.DB, message []byte) ([]byte, error) {
	return nil, nil
}

func handleGetCallByKeyAndPassword(db *gorm.DB, message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	key, keyExists := input["Key"]
	password, passwordExists := input["Password"]

	if !keyExists || !passwordExists {
		return nil, errors.New("password AND key needed")
	}

	var user User
	if err := db.Where(&User{CallKey: key, CallPassword: password}).First(&user).Error; err != nil {
		return nil, err
	}

	var seesion Session
	if err := db.Where(&Session{Model: gorm.Model{ID: user.SessionId}}).First(&seesion).Error; err != nil {
		return nil, err
	}

	result, err := json.Marshal(map[string]string{"AggregatorTopic": seesion.TopicAggregator, "InputsTopic": seesion.TopicInputs})
	if err != nil {
		return nil, err
	}

	if err = db.Model(&seesion).Association("Users").Append(&user); err != nil {
		return nil, err
	}

	return result, nil
}

func handleSetUserActive(db *gorm.DB, message []byte) ([]byte, error) {
	return nil, nil
}

func handleSetUserInactive(db *gorm.DB, message []byte) ([]byte, error) {
	return nil, nil
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
	return nil, nil
}

func handleDisconnect(db *gorm.DB, message []byte) ([]byte, error) {
	return nil, nil
}

func handleRequest(db *gorm.DB, message *Kafka.ConsumerMessage, producer *Kafka.Producer) {
	var sendTopic, operation []byte
	for _, header := range message.Headers {
		switch header.Key {
		case "topic":
			sendTopic = header.Value
		case "operation":
			operation = header.Value
		}
	}

	var err error
	var response []byte
	var responseMessage *Kafka.ProducerMessage
	switch string(operation) {
	case "LOGIN":
		response, err = handleLogin(db, message.Message)
	case "REGISTER":
		response, err = handleRegister(db, message.Message)
	case "ADD_VIDEO":
		response, err = handleAddVideo(db, message.Message)
	case "IS_ACTIVE":
		response, err = handleIsUserActive(db, message.Message)
	case "GET_CALL_BY_KEY":
		response, err = handleGetCallByKeyAndPassword(db, message.Message)
	case "SET_ACTIVE":
		response, err = handleSetUserActive(db, message.Message)
	case "SET_INACTIVE":
		response, err = handleSetUserInactive(db, message.Message)
	case "GET_VIDEOS_BY_USER":
		response, err = handleGetVideoByUser(db, message.Message)
	case "DOWNLOAD_VIDEO_BY_ID":
		response, err = handleDownloadVideoById(db, message.Message)
	case "DISCONNECT":
		response, err = handleDisconnect(db, message.Message)
	default:
		err = errors.New("operation not permitted")
	}
	if err != nil {
		responseMessage = &Kafka.ProducerMessage{
			Message: []byte(err.Error()),
			Topic:   string(sendTopic),
			Headers: []Kafka.Header{
				{`status`, []byte("FAILED")},
			},
		}
	} else {
		responseMessage = &Kafka.ProducerMessage{
			Message: response,
			Topic:   string(sendTopic),
			Headers: []Kafka.Header{
				{`status`, []byte("OK")},
			},
		}
	}

	if err = producer.Publish(responseMessage); err != nil {
		fmt.Println(err)
	} else {
		if len(responseMessage.Message) < 250 {
			fmt.Println("Sent response... ", string(responseMessage.Message))
		}
	}
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
	if err := consumer.SetOffsetToNow(); err != nil {
		panic(err)
	}

	producer := Kafka.NewProducer(brokerAddress)
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	for {
		kafkaMessage, err := consumer.Consume(time.Second * 2)
		if err != nil {
			continue
		}
		fmt.Println("Message...")

		go handleRequest(db, kafkaMessage, producer)
	}
}
