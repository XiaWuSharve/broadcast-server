package kcpserver

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/XiaWuSharve/kcp-webrtc-server/utils"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	registered *utils.ConcurrentMap[string, *Client]
	Cancel     chan struct{}
}

func New() *Server {
	server := &Server{
		registered: utils.NewConcurrentMap[string, *Client](),
		Cancel:     make(chan struct{}),
	}
	return server
}

func (s *Server) Start(listener net.Listener) error {
	slog.Info("server started", "addr", listener.Addr().String())
	for {
		session, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("listener cannot accept kcp: %v", err)
		}
		s.CreateConn(session)
	}
}

func (s *Server) CreateConn(session net.Conn) error {
	c := NewClient(session)

	go func() {
		if err := s.Handle(c); err != nil {
			slog.Error("failed to handle read", "err", err, "id", c.Id)
		}
	}()

	go func() {
		if err := c.HandleSend(); err != nil {
			slog.Error("failed to HandleSend", "err", err, "id", c.Id)
		}
	}()
	go func() {
		if err := c.HandleReceive(); err != nil {
			slog.Error("failed to HandleReceive", "err", err, "id", c.Id)
		}
	}()
	return nil
}

func (s *Server) Handle(c *Client) error {
	slog.Info("started to handle an anonymous client", "remote address", c.Conn.RemoteAddr().String())
	for {
	BEGIN:
		select {
		// TODO 优化优雅关机
		case <-s.Cancel:
			return fmt.Errorf("canceled")
		case frame := <-c.ReadChan:
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
				frame.Len = uint32(len(mess))
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
				frame.Len = uint32(len(mess))
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
				frame.Len = uint32(len(mess))
				frame.Payload = mess
				rc.WriteChan <- frame
				slog.Debug("sent chat message", "id", c.Id, "raw json", mess)
			}
		}
	}
}
