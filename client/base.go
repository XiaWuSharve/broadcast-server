package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/utils"
)

type Conn interface {
	GetReader() io.Reader
	Send(data []byte) error
}

type Client struct {
	Conn
	wg              sync.WaitGroup
	ReceiveProducer mq.Producer[*frame.ReceiveFrame]
	SendConsumer    mq.Consumer[*frame.SendFrame]
	ReceiveFrame    *frame.ReceiveFrame
	HeaderBytes     [13]byte
	streamEncoder   utils.Encoder[*frame.SendFrame]
	streamDecoder   utils.FrameStreamDecoder
	ReceiveErr      error
	SendErr         error
}

var _ mq.Handler[*frame.SendFrame] = (*Client)(nil)

func (c *Client) Start(ctx context.Context) error {
	c.wg.Go(func() {
		if err := c.HandleReceive(ctx); !errors.Is(err, net.ErrClosed) {
			slog.Error("failed to handle receive", "err", c.ReceiveErr)
		}
	})
	c.wg.Wait()
	return nil
}

func (c *Client) HandleReceive(ctx context.Context) error {
	reader := c.GetReader()
	for {
		c.ReceiveFrame, c.ReceiveErr = c.streamDecoder.Parse(reader)
		if c.ReceiveErr != nil {
			if errors.Is(c.ReceiveErr, net.ErrClosed) {
				return c.ReceiveErr
			} else if errors.Is(c.ReceiveErr, utils.ErrTimeLargeOffset) {
				slog.Error(c.ReceiveErr.Error())
			} else {
				return fmt.Errorf("failed to handle receive: %w", c.ReceiveErr)
			}
		}
		_, c.ReceiveErr = c.ReceiveProducer.Enqueue(c.ReceiveFrame)
		if c.ReceiveErr != nil {
			return fmt.Errorf("failed to enqueue: %w", c.ReceiveErr)
		}
		c.ReceiveErr = c.Handle(&frame.SendFrame{
			AckStatus: message.AckStatus_SENDING,
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

func (c *Client) Handle(frame *frame.SendFrame) error {
	c.HeaderBytes[0] = byte(frame.AckStatus)
	binary.BigEndian.PutUint64(c.HeaderBytes[1:9], uint64(frame.ReceiverId))
	binary.BigEndian.PutUint32(c.HeaderBytes[9:13], uint32(len(frame.Payload)))
	slog.Debug("client sending kcp frame")
	c.SendErr = c.Conn.Send(append(c.HeaderBytes[0:13], frame.Payload...))

	// TODO 入刷写队列
	if c.SendErr != nil {
		if errors.Is(c.SendErr, net.ErrClosed) {
			return c.SendErr
		}
		return fmt.Errorf("failed to send message: %w", c.SendErr)
	}
	return nil
}
