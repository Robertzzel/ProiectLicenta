package main

type User struct {
	Id           int
	Name         string
	Password     string
	CallKey      string
	CallPassword string
	SessionId    *int
	Topic        string
}

type Video struct {
	Id                int
	FilePath          string
	DurationInSeconds float64
	Size              string
}

type Session struct {
	Id    int
	Topic string
}
