package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type Conn interface {
	GetId() int64
	GetReader() io.Reader
	GetSendChan() chan *datas.RoutedSendFrame
	Send(data []byte) error
}

type Client struct {
	Conn
	wg              sync.WaitGroup
	Id              int64
	ReceiveProducer mq.Producer[*datas.ReceiveFrame]
	SendConsumer    mq.Consumer[*datas.SendFrame]
	ReceiveFrame    *datas.ReceiveFrame
	HeaderBytes     [13]byte
	// TODO ?
	streamEncoder datas.Encoder[*datas.SendFrame]
	streamDecoder datas.ReceiveFrameStreamDecoder
	ReceiveErr    error
	SendErr       error
	pool          ConnPool
}

var _ mq.Handler[*datas.SendFrame] = (*Client)(nil)

func (c *Client) Start(ctx context.Context) error {
	s.wg.Go(func() {
		if err := s.Handle(ctx, c); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to handle read", "err", err, "id", c.Id)
		}
	})

	s.wg.Go(func() {
		if err := c.HandleSend(ctx); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to HandleSend", "err", err, "id", c.Id)
		}
	})
	s.wg.Go(func() {
		if err := c.HandleReceive(ctx); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to HandleReceive", "err", err, "id", c.Id)
		}
	})

	c.wg.Go(func() {
		if err := c.HandleReceive(ctx); !errors.Is(err, net.ErrClosed) {
			slog.Error("failed to handle receive", "err", c.ReceiveErr)
		}
	})
	c.pool.AddConn(c.Conn)
	return nil
}

func (c *Client) HandleReceive(ctx context.Context) error {
	// 只处理与业务无关的连接相关的逻辑
	reader := bufio.NewReader(c.GetReader())
	for {
		c.ReceiveFrame, c.ReceiveErr = c.streamDecoder.Parse(reader)
		if c.ReceiveErr != nil {
			if errors.Is(c.ReceiveErr, net.ErrClosed) {
				return c.ReceiveErr
			} else if errors.Is(c.ReceiveErr, datas.ErrTimeLargeOffset) {
				slog.Error(c.ReceiveErr.Error())
			} else {
				return fmt.Errorf("failed to handle receive: %w", c.ReceiveErr)
			}
		}
		_, c.ReceiveErr = c.ReceiveProducer.Enqueue(c.ReceiveFrame)
		if c.ReceiveErr != nil {
			return fmt.Errorf("failed to enqueue: %w", c.ReceiveErr)
		}
		c.ReceiveErr = c.Handle(&datas.SendFrame{
			AckStatus: datas.AckStatus_SENDING,
		})
		if c.ReceiveErr != nil {
			if errors.Is(c.ReceiveErr, net.ErrClosed) {
				return c.ReceiveErr
			}
			return fmt.Errorf("cannot ack sending: %w", c.ReceiveErr)
		}
	}
}

func (c *Client) HandleSend(ctx context.Context) error {
	if c.ReceiveErr = c.SendConsumer.Start(c); c.ReceiveErr != nil {
		return fmt.Errorf("failed to start sending consumer: %w", c.ReceiveErr)
	}
	return nil
}
