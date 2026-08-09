package client

import (
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type WebSocketClient struct {
	Client[*websocket.Conn, message.Message]
}

var _ ClientIO[message.Message] = (*WebSocketClient)(nil)

func (c *WebSocketClient) Receive() (*message.Message, error) {
	_, bytes, err := c.Conn.ReadMessage()
	if err != nil {
		if errors.Is(err, websocket.ErrCloseSent) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	var m message.Message
	if err := proto.Unmarshal(bytes, &m); err != nil {
		slog.Error("cannot parse", "err", err, "data", string(bytes))
		return nil, nil
	}
	createdTime := m.GetCreatedTime()
	if !ValidateTime(createdTime) {
		return nil, nil
	}
	return &m, nil
}

func (c *WebSocketClient) Send(mess *message.Message) error {
	m, _ := proto.Marshal(mess)
	return c.Conn.WriteMessage(websocket.BinaryMessage, m)
}

func NewWebSocketClient(session *websocket.Conn) *WebSocketClient {
	return &WebSocketClient{
		Client: Client[*websocket.Conn, message.Message]{
			Conn:      session,
			WriteChan: make(chan *message.Message),
			ReadChan:  make(chan *message.Message),
		},
	}
}
