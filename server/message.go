package server

type Message struct {
	Sender  *Client
	Message string
}
