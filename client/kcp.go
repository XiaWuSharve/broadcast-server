package client

import (
	"io"
	"net"
)

type KcpConn struct {
	net.Conn
	Err error
}

var _ Conn = (*KcpConn)(nil)

func (c *KcpConn) GetReader() io.Reader {
	return c
}
func (c *KcpConn) Send(data []byte) error {
	_, c.Err = c.Write(data)
	return c.Err
}
