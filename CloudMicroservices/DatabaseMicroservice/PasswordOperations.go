package main

import (
	"crypto/sha256"
	"fmt"
)

func Hash(password string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(password)))
}
