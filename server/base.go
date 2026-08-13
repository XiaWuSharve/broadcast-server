package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/kcp-webrtc-server/client"
	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/XiaWuSharve/kcp-webrtc-server/utils"
)

type Server[M any] interface {
	Listen(ctx context.Context, listener net.Listener) error
	TransformRequest(data *M) (int64, *message.Message, error)
	TransformResponse(createdTime int64, mess *message.Message) *M
}

type ServerBase[ConnType any, MessType any] struct {
	Server[MessType]
	registered *utils.ShardMap[string, *client.Client[ConnType, MessType]]
	wg         sync.WaitGroup
}

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
		slog.Debug("received raw message")
		createdTime, m, err := s.TransformRequest(frame)
		if err != nil {
			slog.Error("cannot TransformData", "err", err)
			continue
		}
		switch m.GetData().(type) {
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
		case *message.Message_ConnectMessageRequest:
			// TODO password, current: id as password
			connMess := m.GetConnectMessageRequest()
			c.Id = connMess.GetId()
			c.DisplayName = connMess.GetDisplayName()
			s.registered.Set(c.Id, c)
			m.Data = &message.Message_ConnectMessageResponse{
				ConnectMessageResponse: &message.ConnectMessageResponse{
					Status: message.ConnectStatus_SUCCESS,
				},
			}
			c.WriteChan <- s.TransformResponse(createdTime, m)
			slog.Info("registered", "id", c.Id)
		case *message.Message_CandidateMessage:
			candiMess := m.GetCandidateMessage()
			rc, ok := s.registered.Get(candiMess.GetRemoteId())
			if !ok {
				slog.Error("remote id not found", "remote id", candiMess.GetRemoteId())
				continue
			}
			candiMess.RemoteId = c.Id
			rc.WriteChan <- s.TransformResponse(createdTime, m)
		/**
		{
			"type": "chat",
			"created_time": 1784702839287,
			"data": {
				"remote_id": "commie",
				"message_chain": [
					{ "type": "text", "message": "hello commie" },
					{ "type": "call", "message": "hello call" },
					{ "type": "answer", "message": "hello answer" },
					{ "type": "establish", "message": "hello establish" }
				]
			}
		}
		**/
		case *message.Message_ChatMessage:
			chatMess := m.GetChatMessage()
			rc, ok := s.registered.Get(chatMess.RemoteId)
			if !ok {
				slog.Error("remote id not found", "remote id", chatMess.RemoteId, "local id", c.Id)
				continue
			}
			for _, v := range chatMess.MessageChain {
				if v.Type != message.MessageUnitType_TEXT &&
					v.Type != message.MessageUnitType_CALL &&
					v.Type != message.MessageUnitType_ANSWER &&
					v.Type != message.MessageUnitType_ESTABLISH {
					slog.Error("ilegal message unit type", "got", v.Type)
					break BEGIN
				}
			}
			chatMess.RemoteId = c.Id
			chatMess.DisplayName = c.DisplayName
			rc.WriteChan <- s.TransformResponse(createdTime, m)
			slog.Debug("sent chat message", "id", c.Id, "raw json", m)
		case *message.Message_CallSdp:
			sdpMess := m.GetCallSdp()
			rc, ok := s.registered.Get(sdpMess.RemoteId)
			if !ok {
				slog.Error("remote id not found", "remote id", sdpMess.RemoteId, "local id", c.Id)
				continue
			}
			sdpMess.RemoteId = c.Id
			rc.WriteChan <- s.TransformResponse(createdTime, m)
		case *message.Message_AnswerSdp:
			sdpMess := m.GetAnswerSdp()
			rc, ok := s.registered.Get(sdpMess.RemoteId)
			if !ok {
				slog.Error("remote id not found", "remote id", sdpMess.RemoteId, "local id", c.Id)
				continue
			}
			sdpMess.RemoteId = c.Id
			rc.WriteChan <- s.TransformResponse(createdTime, m)
		}
	}
	return nil
}
