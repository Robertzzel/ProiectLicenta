package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"gopkg.in/vansante/go-ffprobe.v2"
	"gorm.io/gorm"
	"os"
	"regexp"
	"strconv"
)

func HandleLogin(db *gorm.DB, message []byte) ([]byte, error) {
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
	if err := db.First(&user, "name = ? and password = ?", name, Hash(password)).Error; err != nil {
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

func HandleRegister(db *gorm.DB, message []byte) ([]byte, error) {
	user, err := getUserFromMessage(message)
	if err != nil {
		return nil, err
	}

	if len(user.Password) <= 4 || !regexp.MustCompile(`\d`).MatchString(user.Password) {
		return nil, errors.New("password mush have a number, a character and be at least the size of 4")
	}

	user.Password = Hash(user.Password)
	user.CallKey = uuid.NewString()

	if err = db.Create(&user).Error; err != nil {
		return nil, err
	}

	return []byte("Successfully created"), nil
}

func HandleAddVideo(db *gorm.DB, message []byte, sessionId int) ([]byte, error) {
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

func HandleGetCallByKeyAndPassword(db *gorm.DB, message []byte) ([]byte, error) {
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

func HandleGetVideoByUser(db *gorm.DB, message []byte) ([]byte, error) {
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

func HandleDownloadVideoById(db *gorm.DB, message []byte) ([]byte, error) {
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

func HandleDisconnect(db *gorm.DB, message []byte) ([]byte, error) {
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

func HandleUsersInSession(db *gorm.DB, message []byte) ([]byte, error) {
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

func HandleCreateSession(db *gorm.DB, message []byte) ([]byte, error) {
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

func HandleDeleteSession(db *gorm.DB, message []byte) ([]byte, error) {
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

func getUserFromMessage(message []byte) (User, error) {
	var jsonUser JsonUser
	if err := json.Unmarshal(message, &jsonUser); err != nil {
		return User{}, err
	}
	return jsonUser.ToUser(), nil
}
