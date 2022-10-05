package main

import (
	"fmt"
	"time"
)

func main() {
	s := time.Now()
	ts := time.Now().Unix()
	fmt.Println(time.Since(s))
	time.Sleep(time.Second)
	fmt.Println(time.Since(s))
	fmt.Print(ts)
}
