package server

import "github.com/gorilla/websocket"

type Message struct {
	Sender  *websocket.Conn
	Message string
	Err     error
}
