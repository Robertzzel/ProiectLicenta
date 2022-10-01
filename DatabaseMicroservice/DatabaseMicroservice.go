package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

const (
	tableName  = "videos"
	dbNamePath = "./videos.db"
)

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
		log.Fatal("Deschidere")
	}

	if err = db.createTable(); err != nil {
		log.Fatal(err)
	}

	if err = db.Insert("sal10", "sal11"); err != nil {
		log.Println(err)
	}

	rows, err := db.GetAllRows()

	for _, row := range rows {
		fmt.Println(row[0], row[1])
	}
}
