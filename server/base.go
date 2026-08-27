package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/whisperly/client"
)

type Server interface {
	Start(context.Context, net.Listener) error
}

type Listener interface {
	Listen(ctx context.Context, listener net.Listener) error
}

type ServerBase[ConnType any] struct {
	Listener
	conn_pool client.ConnPool
	wg        sync.WaitGroup
}

var _ Server = (*ServerBase[any])(nil)

func (s *ServerBase[C]) Start(ctx context.Context, listener net.Listener) error {
	slog.Info("server started", "addr", listener.Addr().String())
	go func() {
		<-ctx.Done()
		s.wg.Wait()
		listener.Close()
	}()
	if err := s.Listen(ctx, listener); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		return fmt.Errorf("failed to listen: %w", err)
	}
	return nil
}

func (s *ServerBase[C]) CreateConn(ctx context.Context, conn client.Conn) {
	s.conn_pool.AddConn(conn)
	c := client.Client{Conn: conn}
	c.Start(ctx)
}

// TODO 业务解耦
// func (s *ServerBase[C]) Handle(ctx context.Context, c *client.Client) error {
// 	slog.Info("started to handle an anonymous client")
// BEGIN:
// 	// TODO 能否每个data都启动一个goroutine？
// 	for frame := range c.ReadChan {
// 	}
// 	return nil
// }
