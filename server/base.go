package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/utils"
)

type Server interface {
	Start(context.Context, net.Listener) error
}

type Listener interface {
	Listen(ctx context.Context, listener net.Listener) error
}

type DataTransformer[M any] interface {
	TransformRequest(data *M) (int64, *message.Message, error)
	TransformResponse(createdTime int64, mess *message.Message) *M
}

type ServerBase[ConnType any, MessType any] struct {
	Listener
	DataTransformer[MessType]
	registered *utils.ShardMap[string, *client.Client[ConnType, MessType]]
	wg         sync.WaitGroup
}

var _ Server = (*ServerBase[struct{}, struct{}])(nil)

func (s *ServerBase[C, M]) Start(ctx context.Context, listener net.Listener) error {
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

func (s *ServerBase[C, M]) CreateConn(ctx context.Context, c *client.Client[C, M]) {

	s.wg.Go(func() {
		if err := s.Handle(ctx, c); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to handle read", "err", err, "id", c.Id)
		}
	})

	s.wg.Go(func() {
		if err := c.HandleSend(ctx); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to HandleSend", "err", err, "id", c.Id)
		}
	})
	s.wg.Go(func() {
		if err := c.HandleReceive(ctx); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("failed to HandleReceive", "err", err, "id", c.Id)
		}
	})
}

// TODO 业务解耦
func (s *ServerBase[C, M]) Handle(ctx context.Context, c *client.Client[C, M]) error {
	slog.Info("started to handle an anonymous client")
BEGIN:
	// TODO 能否每个data都启动一个goroutine？
	for frame := range c.ReadChan {
	}
	return nil
}
