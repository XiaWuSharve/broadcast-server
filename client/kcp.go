package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/frame"
)

type KcpClient struct {
	Client[net.Conn, frame.Message]
	Rbuf [12]byte
	Wbuf [12]byte
}

var _ ClientIO[frame.Message] = (*KcpClient)(nil)

func (c *KcpClient) Receive() (*frame.Message, error) {
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
	if !ValidateTime(createdTime) {
		return nil, nil
	}
	return &frame.Message{
		CreatedTime: createdTime,
		Payload:     payload,
	}, nil
}

func (c *KcpClient) Send(mess *frame.Message) error {
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
	return &KcpClient{
		Client: Client[net.Conn, frame.Message]{
			Conn:      session,
			WriteChan: make(chan *frame.Message),
			ReadChan:  make(chan *frame.Message),
		},
	}
}
