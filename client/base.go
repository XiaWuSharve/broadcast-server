package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/utils"
)

type ClientIO interface {
	Receive() (*frame.ReceiveFrame, error)
	Send(mess *frame.SendFrame) error
}

type Client[ConnType any] struct {
	ClientIO
	Conn            ConnType
	ReceiveProducer mq.Producer[*frame.ReceiveFrame]
	SendConsumer    mq.Consumer[*frame.SendFrame]
	Data            *frame.ReceiveFrame
	Err             error
	wg              sync.WaitGroup
	encoder         utils.Encoder[*frame.SendFrame]
	SendBytes       []byte
}

var _ mq.Handler[*frame.SendFrame] = (*Client[any])(nil)

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

func (c *Client[C]) Start(ctx context.Context) error {
	c.wg.Go(func() {
		if err := c.HandleReceive(ctx); !errors.Is(err, net.ErrClosed) {
			slog.Error("failed to handle receive", "err", c.Err)
		}
	})
	c.wg.Wait()
}

func (c *Client[ConnType]) HandleReceive(ctx context.Context) error {
	for {
		c.Data, c.Err = c.Receive()
		if c.Err != nil {
			if errors.Is(c.Err, net.ErrClosed) {
				return c.Err
			}
			return fmt.Errorf("failed to handle receive: %w", c.Err)
		}
		_, c.Err = c.ReceiveProducer.Enqueue(c.Data)
		if c.Err != nil {
			return fmt.Errorf("failed to enqueue: %w", c.Err)
		}
		c.Err = c.Send(&frame.SendFrame{
			Ack: message.AckStatus_SENDING,
		})
		if c.Err != nil {
			if errors.Is(c.Err, net.ErrClosed) {
				return c.Err
			}
			return fmt.Errorf("cannot ack sending: %w", c.Err)
		}
	}
}

func (c *Client[ConnType]) HandleSend(ctx context.Context) error {
	// TODO 入刷写队列
	if err := c.SendConsumer.Start(); err != nil {

	}
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

var ErrOffline

func (c *Client[ConnType]) Handle(frame *frame.SendFrame) error {
	if err := c.Send(frame); err != nil {
		if errors.Is(err, )
	}
}
