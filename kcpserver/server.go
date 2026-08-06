package kcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/XiaWuSharve/kcp-webrtc-server/utils"
	"github.com/cespare/xxhash"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	registered *utils.ShardMap[string, *Client]
	wg         sync.WaitGroup
}

func New() *Server {
	server := &Server{
		// TODO config file
		registered: utils.NewShardMap[string, *Client](128, func(k string) uint64 { return xxhash.Sum64([]byte(k)) }),
	}
	return server
}

func (s *Server) Start(ctx context.Context, listener net.Listener) error {
	slog.Info("server started", "addr", listener.Addr().String())
	go func() {
		<-ctx.Done()
		s.wg.Wait()
		listener.Close()
	}()
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
			listener.Close()
		})
		s.CreateConn(ctx, session)
	}
}

func (s *Server) CreateConn(ctx context.Context, session net.Conn) {
	c := NewClient(session)

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
func (s *Server) Handle(ctx context.Context, c *Client) error {
	slog.Info("started to handle an anonymous client", "remote address", c.Conn.RemoteAddr().String())
BEGIN:
	for frame := range c.ReadChan {
		var m message.Message
		if err := proto.Unmarshal(frame.Payload, &m); err != nil {
			slog.Error("cannot parse", "err", err)
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
			mess, _ := proto.Marshal(&m)
			frame.Payload = mess
			c.WriteChan <- frame
			slog.Info("registered", "id", c.Id, "remote address", c.Conn.RemoteAddr().String())
		case *message.Message_CandidateMessage:
			candiMess := m.GetCandidateMessage()
			rc, ok := s.registered.Get(candiMess.GetRemoteId())
			if !ok {
				slog.Error("remote id not found", "remote id", candiMess.GetRemoteId())
				continue
			}
			candiMess.RemoteId = c.Id
			mess, _ := proto.Marshal(&m)
			frame.Payload = mess
			rc.WriteChan <- frame
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
			mess, _ := proto.Marshal(&m)
			frame.Payload = mess
			rc.WriteChan <- frame
			slog.Debug("sent chat message", "id", c.Id, "raw json", mess)
		case *message.Message_CallSdp:
			sdpMess := m.GetCallSdp()
			rc, ok := s.registered.Get(sdpMess.RemoteId)
			if !ok {
				slog.Error("remote id not found", "remote id", sdpMess.RemoteId, "local id", c.Id)
				continue
			}
			sdpMess.RemoteId = c.Id
			mess, _ := proto.Marshal(&m)
			frame.Payload = mess
			rc.WriteChan <- frame
		case *message.Message_AnswerSdp:
			sdpMess := m.GetAnswerSdp()
			rc, ok := s.registered.Get(sdpMess.RemoteId)
			if !ok {
				slog.Error("remote id not found", "remote id", sdpMess.RemoteId, "local id", c.Id)
				continue
			}
			sdpMess.RemoteId = c.Id
			mess, _ := proto.Marshal(&m)
			frame.Payload = mess
			rc.WriteChan <- frame
		}
	}
	return nil
}
