package server

import (
	"fmt"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn        *websocket.Conn
	Id          string
	DisplayName string
	WriteChan   chan []byte
	ReadChan    chan []byte
}

func (c *Client) Receive() ([]byte, error) {
	_, bytes, err := c.Conn.ReadMessage()
	return bytes, err
}

func (c *Client) Send(mess []byte) error {
	return c.Conn.WriteMessage(websocket.BinaryMessage, []byte(mess))
}

func (c *Client) HandleReceive() error {
	for {
		data, err := c.Receive()
		if err != nil {
			return fmt.Errorf("failed to handle receive: %w", err)
		}
		c.ReadChan <- data
	}
}

func (c *Client) HandleSend() error {
	for data := range c.WriteChan {
		if err := c.Send(data); err != nil {
			return fmt.Errorf("failed to handle Send: %w", err)
		}
	}
	return nil
}
