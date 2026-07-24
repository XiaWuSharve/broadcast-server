package kcpserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/frame"
)

type Client struct {
	Conn        net.Conn
	Id          string
	DisplayName string
	WriteChan   chan *frame.Message
	ReadChan    chan *frame.Message
	Rbuf        [12]byte
	Wbuf        [12]byte
}

func NewClient(session net.Conn) *Client {
	return &Client{
		Conn:      session,
		Id:        "",
		WriteChan: make(chan *frame.Message),
		ReadChan:  make(chan *frame.Message),
	}
}

func (c *Client) Receive() (*frame.Message, error) {
	// 改为读Header
	// |CreatedTime 8B|Len 4B -> 1Unit = 1B|
	if _, err := io.ReadFull(c.Conn, c.Rbuf[:]); err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}
	createdTime := int64(binary.BigEndian.Uint64(c.Rbuf[:8]))
	n := binary.BigEndian.Uint32(c.Rbuf[8:])
	slog.Debug("received", "header", c.Rbuf[:], "created time", time.UnixMilli(createdTime).String(), "payload length (Bytes)", n)
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.Conn, payload); err != nil {
		return nil, fmt.Errorf("failed to read payload: %v", err)
	}
	now := time.Now().UnixMilli()
	diff := createdTime - now
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*60*1000 { // 5分钟对应的毫秒数
		slog.Error("client time too slow or fast", "client time", time.UnixMilli(createdTime).Format(time.DateTime))
		return nil, nil
	}
	return &frame.Message{
		CreatedTime: createdTime,
		Len:         n,
		Payload:     payload,
	}, nil
}

func (c *Client) Send(mess *frame.Message) error {
	binary.BigEndian.PutUint64(c.Wbuf[:8], uint64(mess.CreatedTime))
	binary.BigEndian.PutUint32(c.Wbuf[8:], mess.Len)
	_, err := c.Conn.Write(append(c.Wbuf[:], mess.Payload...))
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}
	return nil
}

func (c *Client) HandleReceive() error {
	for {
		data, err := c.Receive()
		if err != nil {
			return fmt.Errorf("failed to handle receive: %v", err)
		}
		if data != nil {
			c.ReadChan <- data
		}
	}
}

func (c *Client) HandleSend() error {
	for data := range c.WriteChan {
		if err := c.Send(data); err != nil {
			return fmt.Errorf("failed to handle Send: %v", err)
		}
	}
	return nil
}
