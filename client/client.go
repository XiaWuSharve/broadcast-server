package client

import "github.com/gorilla/websocket"

type Client struct {
	dialer *websocket.Dialer
}
