package main

import "net"

func main() {
	nets, err := net.LookupHost("robert-Lenovo-ideapad-310-15ISK")
	if err != nil {
		panic(err)
	}
	println(nets[0])
}
