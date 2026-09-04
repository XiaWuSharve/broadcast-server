package client

import (
	"io"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
)

type KcpConn struct {
	net.Conn
	Err      error
	SendChan chan *datas.RoutedSend
	Id       int64
}

// GetSendChan implements [Conn].
func (c *KcpConn) GetSendChan() chan *datas.RoutedSend {
	return c.SendChan
}

var _ Conn = (*KcpConn)(nil)

func (c *KcpConn) GetReader() io.Reader {
	return c
}
func (c *KcpConn) Send(data []byte) error {
	_, c.Err = c.Write(data)
	return c.Err
}
func (c *KcpConn) GetId() int64 {
	return c.Id
}
