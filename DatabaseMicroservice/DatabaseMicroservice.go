package main

import (
	. "Licenta/SocketFunctions"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net"
	"os"
	"strings"
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

func cleanSocket(socketPath string) error {
	if _, err := os.Stat(socketName); !errors.Is(err, os.ErrNotExist) {
		if err := os.Remove(socketName); err != nil {
			return err
		}
	}
	return nil
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

func handleConnection(database *Database, conn net.Conn) {
	for {
		message, err := ReceiveMessage(conn)
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
		} else if strings.HasPrefix(string(message), "insert") {
			insertParts := strings.Split(string(message), ";")
			pathAndTimestamp := strings.Split(insertParts[1], ",")
			if err := database.Insert(pathAndTimestamp[0], pathAndTimestamp[1]); err != nil {
				result = err.Error()
			} else {
				result = "success"
			}
		}

		if err = SendMessage(conn, []byte(result)); err != nil {
			return
		}
	}
}

func main() {
	if err := cleanSocket(socketName); err != nil {
		log.Fatal("Cleanign socket ", err)
	}

	db, err := OpenNewDatabase()
	if err != nil {
		log.Fatal("Deschidere", err)
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
