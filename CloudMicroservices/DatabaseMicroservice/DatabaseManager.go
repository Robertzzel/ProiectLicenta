package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gopkg.in/vansante/go-ffprobe.v2"
	"os"
	"regexp"
	"strconv"
	"time"
)

type DatabaseManager struct {
	*sql.DB
}

func NewDatabaseManager(driverName, connectionString string) (*DatabaseManager, error) {
	db, err := sql.Open(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	return &DatabaseManager{db}, nil
}

func (db *DatabaseManager) UserExists(username, password string) bool {
	var cnt int
	err := db.QueryRow(`select count(*) from User where Name = ? and Password = ? LIMIT 1`, username, Hash(password)).Scan(&cnt)
	if err != nil {
		return false
	}
	return cnt == 1
}

func (db *DatabaseManager) HandleLogin(message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	name, nameExists := input["Name"]
	password, passwordExists := input["Password"]
	topic, topicExists := input["Topic"]

	if !(nameExists && passwordExists && topicExists) {
		return nil, errors.New("name, password are needed")
	}

	// cauta in functie de nume si parola
	var user User
	err := db.QueryRow(`select * from User where Name = ? and Password = ? LIMIT 1`, name, Hash(password)).Scan(&user.Id, &user.Name, &user.Password,
		&user.CallKey, &user.CallPassword, &user.Topic, &user.SessionId)
	if err != nil {
		return nil, errors.New("nume sau parola incorecta")
	}

	user.CallPassword = uuid.NewString()
	user.SessionId = nil
	// creeaza parola noua de call
	_, err = db.Exec(`update User set CallPassword = ?, Topic = ?, SessionId = NULL where Id = ?`, user.CallPassword, topic, user.Id)
	if err != nil {
		return nil, err
	}

	// returneaza user
	return json.Marshal(user)
}

func (db *DatabaseManager) HandleRegister(message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	name, nameExists := input["Name"]
	password, passwordExists := input["Password"]

	if !(nameExists && passwordExists) {
		return nil, errors.New("name, password and topic needed")
	}

	if len(password) <= 4 || !regexp.MustCompile(`\d`).MatchString(password) {
		return nil, errors.New("password mush have a number, a character and be at least the size of 4")
	}

	_, err := db.Exec("insert into User (Name, Password, CallKey, CallPassword, Topic, SessionId) values (?, ?, ?, ?, ?, ?)", name, Hash(password), uuid.NewString(), uuid.NewString(), "", nil)
	if err != nil {
		return nil, err
	}

	return []byte("success"), nil
}

func (db *DatabaseManager) HandleAddVideo(message []byte, sessionId int) ([]byte, error) {
	// creaza fisierul
	filePath, err := WriteNewFile(message)
	if err != nil {
		return nil, err
	}

	//ia detaliile videoclipului
	videoDetails, err := ffprobe.ProbeURL(context.Background(), filePath)
	if err != nil {
		return nil, err
	}

	duration := videoDetails.Format.DurationSeconds
	size := videoDetails.Format.Size

	//creeeaza linia in db
	videoResult, err := db.Exec("insert into Video (FilePath, Duration, CreatedAt, Size) VALUES (?,?, NOW() ,?)", filePath, duration, size)
	if err != nil {
		return nil, err
	}
	videoId, err := videoResult.LastInsertId()
	if err != nil {
		return nil, err
	}

	//pune toti userii care au dreptul la video
	rows, err := db.Query("select Id from User where SessionId = ?", sessionId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uId int
	for rows.Next() {
		err = rows.Scan(&uId)
		if err != nil {
			return nil, err
		}
		if _, err = db.Exec("insert into UserVideo (UserId, VideoId) values (?, ?)", uId, videoId); err != nil {
			return nil, err
		}
	}
	return []byte("successs"), nil
}

func (db *DatabaseManager) HandleGetCallByKeyAndPassword(message []byte) ([]byte, error) {
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

	// gasese sesiunea
	var sessionId *int
	if err = db.QueryRow("select SessionId from User where CallKey = ? and CallPassword = ?", key, password).Scan(&sessionId); err != nil {
		return nil, err
	}

	// daca sharerul nu are sesiune atunci nu e activ, retueaza
	if sessionId == nil {
		return nil, errors.New("the user is not active")
	}

	// gaseste topicul sesinii si creeaza mesajul
	var topic string
	var id int
	err = db.QueryRow("select Topic, Id from Session where Id = ?", *sessionId).Scan(&topic, &id)
	if err != nil {
		return nil, err
	}

	// adauga callerul la sesiune
	_, err = db.Exec("update User set SessionId = ? where Id = ?", *sessionId, callerId)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"Topic": topic, "SessionId": fmt.Sprint(id)})
}

func (db *DatabaseManager) HandleGetVideosByUser(message []byte) ([]byte, error) {
	userId, err := strconv.Atoi(string(message))
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("select Id, Duration, CreatedAt, Size from Video inner join UserVideo on UserVideo.VideoId = Video.Id where UserId = ?", userId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var size string
	var duration float64
	var videoId int
	var tm time.Time
	videos := make([]map[string]string, 0)
	for rows.Next() {
		if err = rows.Scan(&videoId, &duration, &tm, &size); err != nil {
			return nil, err
		}
		videos = append(videos, map[string]string{"ID": strconv.Itoa(videoId), "Duration": fmt.Sprintf("%f", duration), "Size": size,
			"CreatedAt": tm.String()})
	}
	return json.Marshal(videos)
}

func (db *DatabaseManager) HandleDownloadVideoById(message []byte) ([]byte, error) {
	videoId, err := strconv.Atoi(string(message))
	if err != nil {
		return nil, err
	}

	var path string
	err = db.QueryRow("select FilePath from Video where Id = ?", videoId).Scan(&path)
	if err != nil {
		return nil, err
	}

	videoContents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return videoContents, nil
}

func (db *DatabaseManager) HandleCreateSession(message []byte) ([]byte, error) {
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

	res, err := db.Exec("insert into Session(Topic) values (?)", topic)
	if err != nil {
		return nil, err
	}

	sessionId, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec("update User set SessionId = ? where Id = ?", sessionId, userId); err != nil {
		return nil, err
	}

	return []byte(strconv.FormatInt(sessionId, 10)), nil
}

func (db *DatabaseManager) HandleDeleteSession(sessionId int) ([]byte, error) {
	if _, err := db.Exec("update User set SessionId = NULL where SessionId = ?", sessionId); err != nil {
		return nil, err
	}

	res, err := db.Exec("delete from Session where Id = ?", sessionId)
	if err != nil {
		return nil, err
	} else {
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, errors.New("Deleted " + strconv.FormatInt(affected, 10) + " rows")
		}
	}

	return []byte("success"), nil
}

func (db *DatabaseManager) GetUserTopicsBySession(sessionId int) ([]byte, error) {
	rows, err := db.Query("select Topic from User where SessionId = ?", sessionId)

	var topic string
	topics := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&topic)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}

	return json.Marshal(topics)
}

func (db *DatabaseManager) ChangeUserPassword(message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	username, usernameExists := input["username"]
	password, passwordExists := input["password"]
	newPassword, newPasswordExists := input["newPassword"]

	if !(usernameExists && passwordExists && newPasswordExists) {
		return nil, errors.New("name, old password and new password are needed")
	}

	res, err := db.Exec("UPDATE User SET Password = ? WHERE Name = ? AND Password = ?", Hash(newPassword), username, Hash(password))
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, errors.New("no user found for given username and password")
	}

	return []byte("success"), nil
}

func (db *DatabaseManager) DeleteVideo(message []byte) ([]byte, error) {
	var input map[string]string
	if err := json.Unmarshal(message, &input); err != nil {
		return nil, err
	}

	userid, userIdExists := input["userId"]
	videoId, videoIdExists := input["videoId"]
	if !(videoIdExists && userIdExists) {
		return nil, errors.New("videoId is needed")
	}

	res, err := db.Exec("DELETE FROM UserVideo WHERE UserId = ? AND VideoId = ?", userid, videoId)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, errors.New("no video referenced deleted")
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM UserVideo WHERE VideoId = ?", videoId).Scan(&count)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		var filepath string
		err = db.QueryRow("SELECT FilePath FROM Video WHERE Id = ?", videoId).Scan(&filepath)
		if err != nil {
			fmt.Println("Error while getting video filepath", videoId)
			return []byte("success"), nil
		}

		res, err = db.Exec("DELETE FROM Video WHERE Id = ?", videoId)
		if err != nil {
			fmt.Println("Error while deletting video", videoId)
			return []byte("success"), nil
		}
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			return nil, err
		}

		if rowsAffected == 0 {
			fmt.Println("No video row found for id", videoId)
			return []byte("success"), nil
		}

		err = os.Remove(filepath)
		if err != nil {
			fmt.Println("Cannot delete video, filepath does not exist", videoId)
			return []byte("success"), nil
		}
	}
	return []byte("success"), nil
}

func (db *DatabaseManager) MigrateDatabase() error {
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
		Topic varchar(255) NOT NULL,
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
    CreatedAt DATETIME,
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
