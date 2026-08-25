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
	Reader          io.Reader
	ReceiveProducer mq.Producer[*frame.ReceiveFrame]
	SendConsumer    mq.Consumer[*frame.SendFrame]
	ReceiveFrame    *frame.ReceiveFrame
	ReceiveErr      error
	SendErr         error
	wg              sync.WaitGroup
	encoder         utils.Encoder[*frame.SendFrame]
	SendBytes       []byte
	streamDecoder   utils.FrameStreamDecoder
	frame           *frame.ReceiveFrame
	Err             error
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
	for {
		c.frame, c.ReceiveErr = c.streamDecoder.Parse(c.Reader)
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
	binary.BigEndian.PutUint64(c.Wbuf[:8], uint64(mess.CreatedTime))
	binary.BigEndian.PutUint32(c.Wbuf[8:], uint32(len(mess.Payload)))
	slog.Debug("client sending kcp frame", "bytes", string(append(c.Wbuf[:], mess.Payload...)))
	c.SendErr = c.Conn.Send(append(c.Wbuf[:], mess.Payload...))

	// TODO 入刷写队列
	if c.SendErr != nil {
		if errors.Is(c.SendErr, net.ErrClosed) {
			return c.SendErr
		}
		return fmt.Errorf("failed to send message: %w", c.SendErr)
	}
	return nil
}
