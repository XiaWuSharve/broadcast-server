package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/nsqio/go-nsq"
)

type KcpClient struct {
	Client[net.Conn]
	Rbuf        [12]byte
	Wbuf        [12]byte
	CreatedTime int64
	PayloadLen  int
	Transaction chan *nsq.ProducerTransaction
	Err         error
}

var _ ClientIO[frame.ReceiveFrame] = (*KcpClient)(nil)

func (c *KcpClient) Receive() (*frame.SendFrame, error) {
	// TODO |AckType 1B|CreatedTime 8B|Len 4B -> 1Unit = 1B|
	if _, c.Err = io.ReadFull(c.Conn, c.Rbuf[:]); c.Err != nil {
		if errors.Is(c.Err, io.ErrClosedPipe) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read header: %w", c.Err)
	}
	c.CreatedTime = int64(binary.BigEndian.Uint64(c.Rbuf[0:8]))
	c.PayloadLen = int(binary.BigEndian.Uint32(c.Rbuf[8:12]))
	slog.Debug("received", "header", c.Rbuf[:], "created time", time.UnixMilli(c.CreatedTime).String(), "payload length (Bytes)", c.PayloadLen)
	if !ValidateTime(c.CreatedTime) {
		c.Send(&frame.SendFrame{
			Ack: message.AckStatus_FAIL,
		})
		io.CopyN(io.Discard, c.Conn, int64(c.PayloadLen))
		return nil
	}

	payload := make([]byte, c.PayloadLen)
	if _, c.Err = io.ReadFull(c.Conn, payload); c.Err != nil {
		if errors.Is(c.Err, io.ErrClosedPipe) {
			return net.ErrClosed
		}
		return fmt.Errorf("failed to read payload: %w", c.Err)
	}
	c.Transaction, c.Err = c.Producer.Enqueue(&frame.ReceiveFrame{
		CreatedTime: c.CreatedTime,
		Payload:     payload,
	})
	if c.Err != nil {
		return fmt.Errorf("failed to enqueue: %w", c.Err)
	}
	return nil
}

func (c *KcpClient) Send(mess *frame.SendFrame) error {
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

func NewKcpClient(session net.Conn) *KcpClient {
	c := &KcpClient{
		Client: Client[net.Conn, frame.ReceiveFrame]{
			Conn:      session,
			WriteChan: make(chan *frame.ReceiveFrame),
			ReadChan:  make(chan *frame.ReceiveFrame),
		},
	}
	c.ClientIO = c
	return c
}
