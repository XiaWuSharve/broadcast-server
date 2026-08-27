package client

import (
	"io"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
)

type KcpConn struct {
	net.Conn
	Err      error
	SendChan chan *datas.RoutedSendFrame
	Id       int64
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
