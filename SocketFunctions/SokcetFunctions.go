package SocketFunctions

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
)

const (
	messageSizeLength = 10
)

func ReceiveMessage(connection net.Conn) ([]byte, error) {
	var buffer bytes.Buffer

	if _, err := io.CopyN(&buffer, connection, messageSizeLength); err != nil {
		return nil, err
	}

	messageSize, err := strconv.Atoi(string(buffer.Bytes()))
	if err != nil {
		return nil, err
	}

	buffer.Reset()
	if _, err = io.CopyN(&buffer, connection, int64(messageSize)); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func SendMessage(connection net.Conn, message []byte) error {
	var err error
	buffer := bytes.NewBufferString(fmt.Sprintf("%010d", len(message))) // here you have to modify message size manually

	if _, err = io.Copy(connection, buffer); err != nil {
		return err
	}

	buffer.Reset()
	if _, err = buffer.Write(message); err != nil {
		return err
	}

	_, err = io.Copy(connection, buffer)
	return err
}
