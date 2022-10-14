package main

import (
	"Licenta/Kafka"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"strings"
)

const (
	tableName   = "videos"
	dbNamePath  = "./videos.db"
	MergerTopic = "Merger"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

type Database struct {
	database *sql.DB
}

func OpenNewDatabase() (*Database, error) {
	db, err := sql.Open("sqlite3", dbNamePath)
	if err != nil {
		return nil, err
	}

	return &Database{db}, err
}

func (db *Database) createTable() error {
	statement, err := db.database.Prepare(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (filePath TEXT PRIMARY KEY, dateAdded TEXT)", tableName))
	if err != nil {
		return err
	}

	_, err = statement.Exec()
	return err
}

func (db *Database) Insert(filePath, date string) error {
	statement, err := db.database.Prepare(fmt.Sprintf("INSERT INTO %s (filePath, dateAdded) VALUES (?, ?)", tableName))
	if err != nil {
		return err
	}

	_, err = statement.Exec(filePath, date)
	return err
}

func (db *Database) GetAllRows() ([][2]string, error) {
	rows, err := db.database.Query(fmt.Sprintf("SELECT filePath, dateAdded FROM %s", tableName))
	if err != nil {
		return nil, err
	}

	var path string
	var date string
	var result [][2]string

	for rows.Next() {
		err = rows.Scan(&path, &date)
		if err != nil {
			return nil, err
		}

		result = append(result, [2]string{path, date})
	}

	return result, nil
}

func main() {
	db, err := OpenNewDatabase()
	if err != nil {
		log.Fatal("Deschidere", err)
	}

	if err = db.createTable(); err != nil {
		log.Fatal(err)
	}

	mergerConsumer := Kafka.NewConsumer(MergerTopic)
	defer mergerConsumer.Close()

	for {
		message, err := mergerConsumer.Consume()
		checkErr(err)

		if strings.HasPrefix(string(message.Value), "insert") {
			insertParts := strings.Split(string(message.Value), ";")
			pathAndTimestamp := strings.Split(insertParts[1], ",")
			checkErr(db.Insert(pathAndTimestamp[0], pathAndTimestamp[1]))
			fmt.Println(pathAndTimestamp[0], pathAndTimestamp[1])
		}
	}
}
