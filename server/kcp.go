package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/datas"
)

type KcpServer struct {
	ServerBase[net.Conn]
}

var _ Listener = (*KcpServer)(nil)

func NewKcpServer() *KcpServer {
	server := &KcpServer{
		ServerBase: ServerBase[net.Conn]{},
	}
	server.Listener = server
	return server
}

func (s *KcpServer) Listen(ctx context.Context, listener net.Listener) error {
	for {
		session, err := listener.Accept()
		if err != nil {
			if errors.Is(err, io.ErrClosedPipe) {
				return net.ErrClosed
			}
			return fmt.Errorf("listener cannot accept kcp: %w", err)
		}
		s.wg.Go(func() {
			<-ctx.Done()
			session.Close()
		})
		s.CreateConn(ctx, &client.KcpConn{Conn: session, Id: datas.GenId()})
	}
}
