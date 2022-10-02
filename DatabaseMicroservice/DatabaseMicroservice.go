package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"net"
	"strconv"
)

const (
	tableName  = "videos"
	dbNamePath = "./videos.db"
	socketName = "/tmp/database.sock"
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

func receiveMessage(connection net.Conn) ([]byte, error) {
	sizeBuffer := make([]byte, 10)
	if _, err := io.LimitReader(connection, 10).Read(sizeBuffer); err != nil {
		return nil, err
	}

	size, err := strconv.Atoi(string(sizeBuffer))
	if err != nil {
		return nil, err
	}

	messageBuffer := make([]byte, size)
	if _, err := connection.Read(messageBuffer); err != nil {
		return nil, err
	}

	return messageBuffer, nil
}

func sendMessage(connection net.Conn, message []byte) error {
	if _, err := connection.Write([]byte(fmt.Sprintf("%010d", len(message)))); err != nil {
		return err
	}

	_, err := connection.Write(message)
	return err
}

func handleConnection(database *Database, conn net.Conn) {
	for {
		message, err := receiveMessage(conn)
		if err != nil {
			return
		}

		result := ""
		if string(message) == "query;all" {
			rows, err := database.GetAllRows()
			if err != nil {
				return
			}

			for _, row := range rows {
				result += row[0] + "," + row[1] + ";"
			}
			result = result[:len(result)-1]
		}

		if err = sendMessage(conn, []byte(result)); err != nil {
			return
		}
	}
}

func main() {
	db, err := OpenNewDatabase()
	if err != nil {
		log.Fatal("Deschidere")
	}

	if err = db.createTable(); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("unix", socketName)
	if err != nil {
		log.Fatal("Deschidere socket", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Eroare la accept ", err)
		}

		go handleConnection(db, conn)
	}
}
