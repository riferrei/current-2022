package main

type Person struct {
	UserName       string   `json:"userName"`
	FavoriteNumber int64    `json:"favoriteNumber"`
	Interests      []string `json:"interests"`
}
