package main

import (
	"gorm.io/gorm"
)

/*
type Model struct {
  ID        uint           `gorm:"primaryKey"`
  CreatedAt time.Time
  UpdatedAt time.Time
  DeletedAt gorm.DeletedAt `gorm:"index"`
}
*/

type User struct {
	gorm.Model
	Name         string   `gorm:"unique;not null"`
	Password     string   `gorm:"not null"`
	CallKey      string   `gorm:"unique;not null"`
	CallPassword string   `gorm:"not null"`
	SessionId    uint     `gorm:"default:null"`
	Videos       []*Video `gorm:"many2many:user_videos;"`
}

type Video struct {
	gorm.Model
	FilePath string  `gorm:"unique;not null"`
	Users    []*User `gorm:"many2many:user_videos;"`
}

type Session struct {
	gorm.Model
	TopicAggregator string  `gorm:"not null"`
	TopicInputs     string  `gorm:"not null"`
	Users           []*User `gorm:"foreignKey:SessionId"`
}

type JsonUser struct {
	ID       uint   `json:"ID"`
	Name     string `json:"Name"`
	Password string `json:"Password"`
}

func (inputUser JsonUser) ToUser() User {
	return User{Name: inputUser.Name, Password: inputUser.Password, Model: gorm.Model{ID: inputUser.ID}}
}

type JsonVideo struct {
	FilePath string     `json:"FilePath"`
	Users    []JsonUser `json:"Users"`
	ID       uint       `json:"ID"`
	Content  []byte     `json:"Content"`
}

func (inputVideo JsonVideo) ToVideo() Video {
	users := make([]*User, 0)
	for _, inputUser := range inputVideo.Users {
		user := inputUser.ToUser()
		users = append(users, &user)
	}
	return Video{FilePath: inputVideo.FilePath, Users: users, Model: gorm.Model{ID: inputVideo.ID}}
}
