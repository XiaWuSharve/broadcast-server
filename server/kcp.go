package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/utils"
	"github.com/cespare/xxhash"
)

type KcpServer struct {
	ServerBase[net.Conn]
}

var _ Listener = (*KcpServer)(nil)

func NewKcpServer() *KcpServer {
	server := &KcpServer{
		ServerBase: ServerBase[net.Conn]{
			registered: utils.NewShardMap[string, *client.Client](config.Cfg.RegistryMaxBucketNum, func(k string) uint64 { return xxhash.Sum64([]byte(k)) }),
		},
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
		c := client.Client{
			Conn: &client.KcpConn{Conn: session},
		}
		s.CreateConn(ctx, &c)
	}
}