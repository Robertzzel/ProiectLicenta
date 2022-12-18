package main

import (
	"Licenta/Kafka"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"time"
)

const (
	connectionString = "robert:robert@tcp(localhost:3306)/licenta?parseTime=true"
	topic            = "DATABASE"
)

type User struct {
	gorm.Model
	Name     string `gorm:"unique;not null"`
	Password string
	Videos   []*Video `gorm:"many2many:user_videos;"`
}

type InputUser struct {
	ID       uint   `json:"ID"`
	Name     string `json:"Name"`
	Password string `json:"Password"`
}

func (inputUser InputUser) ToUser() User {
	return User{Name: inputUser.Name, Password: inputUser.Password, Model: gorm.Model{ID: inputUser.ID}}
}

type Video struct {
	gorm.Model
	FilePath string  `gorm:"unique;not null"`
	Users    []*User `gorm:"many2many:user_videos;"`
}

type InputVideo struct {
	FilePath string      `json:"FilePath"`
	Users    []InputUser `json:"Users"`
	ID       uint        `json:"ID"`
}

func (inputVideo InputVideo) ToVideo() Video {
	users := make([]*User, 0)
	for _, inputUser := range inputVideo.Users {
		user := inputUser.ToUser()
		users = append(users, &user)
	}
	return Video{FilePath: inputVideo.FilePath, Users: users, Model: gorm.Model{ID: inputVideo.ID}}
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

func hash(password string) string {
	hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", hash)
}

func handleUserRequest(db *gorm.DB, operation string, input []byte) (*Kafka.ProducerMessage, error) {
	var inputUser InputUser
	if err := json.Unmarshal(input, &inputUser); err != nil {
		return nil, err
	}
	user := inputUser.ToUser()
	user.Password = hash(user.Password)

	switch operation {
	case "CREATE":
		if err := db.Create(&user).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(db.Error.Error())}, nil
		}
	case "READ": // check
		if err := db.Where("name = ? and password = ?", user.Name, user.Password).First(&user).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error()), Headers: []Kafka.Header{{Key: "status", Value: []byte("FAILED")}}}, nil
		}
		return &Kafka.ProducerMessage{Message: []byte(fmt.Sprint(user.ID)), Headers: []Kafka.Header{{Key: "status", Value: []byte("OK")}}}, nil
	case "DELETE":
		if err := db.Delete(&user); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(db.Error.Error())}, nil
		}
	case "UPDATE":
		if err := db.Where("name = ?", user.Name).Update("password", user.Password); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error.Error())}, nil
		}
	}

	return &Kafka.ProducerMessage{Message: []byte("OK")}, nil
}

func handleVideoRequest(db *gorm.DB, operation string, input []byte, data []byte) (*Kafka.ProducerMessage, error) {
	var inputVideo InputVideo
	if err := json.Unmarshal(input, &inputVideo); err != nil {
		return nil, err
	}
	video := inputVideo.ToVideo()
	users := video.Users
	video.Users = nil

	switch operation {
	case "CREATE":
		// create file
		i := 0
		for {
			video.FilePath = fmt.Sprintf("video%d.mp4", i)
			if err := os.WriteFile(video.FilePath, data, 0777); err == nil {
				break
			}
			i++
		}

		if db.Create(&video).Error != nil {
			return &Kafka.ProducerMessage{Message: []byte(db.Error.Error())}, nil
		}

		foundUsers := make([]*User, 0)
		for _, user := range users {
			if db.Model(user).Where("name = ?", user.Name).First(user).Error == nil {
				foundUsers = append(foundUsers, user)
			}
		}

		if err := db.Model(&video).Where("id = ?", video.ID).Association("Users").Append(&foundUsers); err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error())}, nil
		}
	case "READ": // check
		// TODO SEND VIDEO CONTENTS
		video = Video{Model: gorm.Model{ID: inputVideo.ID}}
		if err := db.First(&video).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error())}, nil
		}
	case "DELETE":
		if err := db.Delete(&video).Error; err != nil {
			return &Kafka.ProducerMessage{Message: []byte(err.Error())}, nil
		}
	}
	return &Kafka.ProducerMessage{Message: []byte("OK")}, nil
}

func main() {
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})
	if err != nil {
		panic("cannot open the database")
	}

	if err := db.AutoMigrate(&User{}, &Video{}); err != nil {
		panic(err)
	}

	consumer := Kafka.NewConsumer(topic)
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	producer := Kafka.NewProducer()
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

		var sendTopic, table, operation string
		var input []byte
		for _, header := range kafkaMessage.Headers {
			switch header.Key {
			case "topic":
				sendTopic = string(header.Value)
			case "operation":
				operation = string(header.Value)
			case "table":
				table = string(header.Value)
			case "input":
				input = header.Value
			}
		}

		var resultMessage *Kafka.ProducerMessage

		switch table {
		case "users":
			resultMessage, err = handleUserRequest(db, operation, input)
		case "videos":
			resultMessage, err = handleVideoRequest(db, operation, input, kafkaMessage.Message)
		}
		if err != nil {
			break
		}

		resultMessage.Topic = sendTopic
		if err = producer.Publish(resultMessage); err != nil {
			fmt.Println(err)
			continue
		}
	}
}
