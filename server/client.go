package server

import "github.com/gorilla/websocket"

type Client struct {
	Conn  *websocket.Conn
	WChan chan string
}
