package client

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/gorilla/websocket"
)

type WsConn struct {
	*websocket.Conn
	Err    error
	reader io.Reader
	n      int
}

var _ Conn = (*WsConn)(nil)
var _ io.Reader = (*WsConn)(nil)

func (c *WsConn) Read(p []byte) (int, error) {
	c.n, c.Err = c.reader.Read(p)
	if c.n < len(p) {
		// 读帧头不允许长度比Payload还长
		return c.n, io.EOF
	}
	if c.Err != nil {
		if errors.Is(c.Err, io.EOF) {
			_, c.reader, c.Err = c.Conn.NextReader()
			if c.Err != nil {
				if websocket.IsCloseError(c.Err) {
					return 0, net.ErrClosed
				}
				return 0, fmt.Errorf("failed to read message: %w", c.Err)
			}
			return c.Read(p)
		}
	}
	return c.n, c.Err
}

func (c *WsConn) GetReader() io.Reader {
	return c
}

func (c *WsConn) Send(data []byte) error {
	return c.Conn.WriteMessage(websocket.BinaryMessage, data)
}
