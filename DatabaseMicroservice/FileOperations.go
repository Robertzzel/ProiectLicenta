package main

import (
	"fmt"
	"os"
	"time"
)

func DoesFileExist(fileName string) bool {
	_, err := os.Stat(fileName)
	return !os.IsNotExist(err)
}

func WriteNewFile(data []byte) (string, error) {
	for {
		path := fmt.Sprintf("./video_%d.mp4", time.Now().UnixMilli())
		if DoesFileExist(path) {
			continue
		}
		if err := os.WriteFile(path, data, 0777); err != nil {
			return "", err
		}
		return path, nil
	}
}
