package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"
)

type ClientIO[MessType any] interface {
	Receive() (*MessType, error)
	Send(mess *MessType) error
}

type Client[ConnType any, MessageType any] struct {
	Id          string
	DisplayName string
	Conn        ConnType
	WriteChan   chan *MessageType
	ReadChan    chan *MessageType
	ClientIO[MessageType]
}

func ValidateTime(createdTime int64) bool {
	now := time.Now().UnixMilli()
	diff := createdTime - now
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*60*1000 { // 5分钟对应的毫秒数
		slog.Error("client time too slow or fast", "client time", time.UnixMilli(createdTime).Format(time.DateTime))
		return false
	}
	return true
}

func (c *Client[ConnType, MessageType]) HandleReceive(ctx context.Context) error {
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

func (c *Client[ConnType, MessageType]) HandleSend(ctx context.Context) error {
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
