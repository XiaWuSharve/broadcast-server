package kcpserver

import (
	"context"
	"encoding/binary"
	"errors"
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
		if errors.Is(err, io.ErrClosedPipe) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	createdTime := int64(binary.BigEndian.Uint64(c.Rbuf[:8]))
	n := binary.BigEndian.Uint32(c.Rbuf[8:])
	slog.Debug("received", "header", c.Rbuf[:], "created time", time.UnixMilli(createdTime).String(), "payload length (Bytes)", n)
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.Conn, payload); err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read payload: %w", err)
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
		Payload:     payload,
	}, nil
}

func (c *Client) Send(mess *frame.Message) error {
	binary.BigEndian.PutUint64(c.Wbuf[:8], uint64(mess.CreatedTime))
	binary.BigEndian.PutUint32(c.Wbuf[8:], uint32(len(mess.Payload)))
	slog.Debug("client sending kcp frame", "bytes", string(append(c.Wbuf[:], mess.Payload...)))
	_, err := c.Conn.Write(append(c.Wbuf[:], mess.Payload...))
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		return fmt.Errorf("failed to send message: %w", err)
	}
	return nil
}

func (c *Client) HandleReceive(ctx context.Context) error {
	defer close(c.ReadChan)
	for {
		data, err := c.Receive()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			return fmt.Errorf("failed to handle receive: %w", err)
		}
		if data != nil {
			c.ReadChan <- data
		}
	}
}

func (c *Client) HandleSend(ctx context.Context) error {
	for data := range c.WriteChan {
		if err := c.Send(data); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			return fmt.Errorf("failed to handle Send: %w", err)
		}
	}
	return nil
}
