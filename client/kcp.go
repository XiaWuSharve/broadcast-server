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
	"google.golang.org/protobuf/proto"
)

type KcpClient struct {
	Client[net.Conn, frame.Message]
	Rbuf          [16]byte
	Wbuf          [16]byte
	SenderBuf     [256]byte
	ReceiverBuf   [256]byte
	CONN          bool
	ACK           bool
	CreatedTime   int64
	SenderIdLen   int
	ReceiverIdLen int
	PayloadLen    uint32
	SenderId      string
	ReceiverId    string
}

var _ ClientIO[frame.Message] = (*KcpClient)(nil)

// |CONN 1b|ACK 1b|preserved 6b+1B|sender id len 1B|receiver id len 1B|
// |CreatedTime 8B|
// |payload len 4B|
// |sender id n*4B|
// |receiver id n*4B|
func (c *KcpClient) Receive() error {
	if _, err := io.ReadFull(c.Conn, c.Rbuf[:]); err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			return net.ErrClosed
		}
		return fmt.Errorf("failed to read header: %w", err)
	}
	c.CONN = (c.Rbuf[0] & 0x80) != 0
	// TODO 暂时先不解析
	c.ACK = (c.Rbuf[0] & 0x40) != 0
	c.SenderIdLen = int(c.Rbuf[2])
	c.ReceiverIdLen = int(c.Rbuf[3])
	c.CreatedTime = int64(binary.BigEndian.Uint64(c.Rbuf[4:12]))
	c.PayloadLen = binary.BigEndian.Uint32(c.Rbuf[12:16])
	slog.Debug("received header", "CONN", c.CONN, "ACK", c.ACK, "SenderIdLen", c.SenderIdLen, "ReceiverIdLen", c.ReceiverIdLen,
		"CreatedTime", time.UnixMilli(c.CreatedTime).String(), "payload length (Bytes)", c.PayloadLen)
	// (c.SenderIdLen+3)&^3 向上对齐到4的倍数
	if !ValidateTime(c.CreatedTime) {
		slog.Error("client time too slow or fast", "client time", time.UnixMilli(c.CreatedTime).Format(time.DateTime))
		io.CopyN(io.Discard, c.Conn, ((int64(c.SenderIdLen)+3)&^3)+((int64(c.ReceiverIdLen)+3)&^3)+((int64(c.PayloadLen)+3)&^3))
		return fmt.Errorf("time invalid: %d", c.CreatedTime)
	}
	if _, err := io.ReadFull(c.Conn, c.SenderBuf[:(c.SenderIdLen+3)&^3]); err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			return net.ErrClosed
		}
		return fmt.Errorf("failed to read sender id: %w", err)
	}
	c.SenderId = string(c.SenderBuf[:c.SenderIdLen])
	if c.CONN {
		io.CopyN(io.Discard, c.Conn, (int64(c.ReceiverIdLen)+3)&^3)
	} else {
		if _, err := io.ReadFull(c.Conn, c.ReceiverBuf[:(c.ReceiverIdLen+3)&^3]); err != nil {
			if errors.Is(err, io.ErrClosedPipe) {
				return net.ErrClosed
			}
			return fmt.Errorf("failed to read receiver id: %w", err)
		}
		c.ReceiverId = string(c.ReceiverBuf[:c.ReceiverIdLen])
	}
	payLoadBuf := make([]byte, (c.PayloadLen+3)&^3)
	if _, err := io.ReadFull(c.Conn, payLoadBuf); err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			return net.ErrClosed
		}
		return fmt.Errorf("failed to read receiver id: %w", err)
	}
	var m message.Message
	proto.Unmarshal(payLoadBuf[:c.PayloadLen], &m)
	if c.CONN {
		c.DisplayName = m.GetChatMessage().GetDisplayName()
		c.Registry.Set(c.SenderId, &c.Client)
		// REGISTERED
		c.Send(&message.Message{})
		return nil
	}
	m.SenderId = c.SenderId
	m.ReceiverId = c.ReceiverId
	m.CreatedTime = c.CreatedTime
	transaction, err := c.Producer.Enqueue(&m)
	if err != nil {
		return fmt.Errorf("cannot enqueue receive mq: %w", err)
	}
	go func() {
		t := <-transaction
		if t.Error != nil {
			// FAILED
			c.Send()
		}
		// SENDING
		c.Send()
	}()
	return nil
	/**
	{
		"type": "connect",
		"created_time": 1784702839287,
		"data": {
			"id": "sharve",
			"display_name": "夏午"
		}
	}
	**/
	// case *message.Message_ConnectMessageRequest:
	// // TODO password, current: id as password
	// connMess := m.GetConnectMessageRequest()
	// c.Id = connMess.GetId()
	// c.DisplayName = connMess.GetDisplayName()
	// s.registered.Set(c.Id, c)
	// m.Data = &message.Message_ConnectMessageResponse{
	// 	ConnectMessageResponse: &message.ConnectMessageResponse{
	// 		Status: message.ConnectStatus_SUCCESS,
	// 	},
	// }
	// c.WriteChan <- s.TransformResponse(createdTime, m)
	// slog.Info("registered", "id", c.Id)
}

func (c *KcpClient) Send(mess *message.Message) error {
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
		Client: Client[net.Conn, frame.Message]{
			Conn:      session,
			WriteChan: make(chan *frame.Message),
			ReadChan:  make(chan *frame.Message),
		},
	}
	c.ClientIO = c
	return c
}
