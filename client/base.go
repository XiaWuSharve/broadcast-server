package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/utils"
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
	Registry    *Registry[ConnType, MessageType]
	Producer    *mq.ReceiveProducer
	ClientIO[MessageType]
}

type Registry[ConnType, MessType any] = utils.ShardMap[string, *Client[ConnType, MessType]]

func ValidateTime(createdTime int64) bool {
	diff := createdTime - time.Now().UnixMilli()
	if diff < 0 {
		diff = -diff
	}
	if diff > config.Cfg.TimeTolerance*1000 { // 5分钟对应的毫秒数
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
