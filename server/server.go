package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	upgrader   *websocket.Upgrader
	registered *ConcurrentMap[string, *Client]
	Cancel     chan struct{}
}

func New(rBufSize int, wBufSize int) *Server {
	server := &Server{
		registered: NewConcurrentMap[string, *Client](),
		Cancel:     make(chan struct{}),
	}
	server.setUpgrader(rBufSize, wBufSize)
	return server
}

func (s *Server) Start(httpServer *http.Server) {
	// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "home.html") })
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		err := s.CreateConn(w, r)
		if err != nil {
			slog.Error("cannot create conn", "err", err)
		}
	})
	// TODO using config file
	slog.Info("server started at ws://0.0.0.0:3001/ws")
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("cannot ListenAndServe", "err", err)
	}
}

func (s *Server) setUpgrader(rBufSize int, wBufSize int) {
	s.upgrader = &websocket.Upgrader{
		ReadBufferSize:  rBufSize,
		WriteBufferSize: wBufSize,
	}
}

func (s *Server) CreateConn(w http.ResponseWriter, r *http.Request) error {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return fmt.Errorf("server: unable to upgrade the HTTP server connection to the WebSocket protocol: %s", err)
	}

	c := &Client{
		Conn:      conn,
		Id:        "",
		WriteChan: make(chan dto.Message),
		ReadChan:  make(chan dto.Message),
	}
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
		case <-s.Cancel:
			return fmt.Errorf("canceled")
		case data := <-c.ReadChan:
			slog.Debug("received", "raw message", string(data))
			// 改为读Header
			// |Type 2B|CreatedTime 8B|Len 4B -> 1Unit = 1B|
			var message dto.Message
			if err := json.Unmarshal(data, &message); err != nil {
				slog.Error("cannot parse", "err", err, "data", string(data))
				continue
			}
			timestamp := message.CreatedTime
			now := time.Now().UnixMilli()
			diff := timestamp - now
			if diff < 0 {
				diff = -diff
			}
			if diff > 5*60*1000 { // 5分钟对应的毫秒数
				slog.Error("client time too slow or fast", "client time", time.UnixMilli(timestamp).Format(time.DateTime))
				continue
			}
			switch message.Type {
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
			case dto.MessageType_CONNECT:
				var req dto.ConnectMessageRequest
				if err := proto.Unmarshal(data.Payload, &req); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
					continue
				}
				// TODO password, current: id as password
				c.Id = req.Id
				c.DisplayName = req.DisplayName
				s.registered.Set(c.Id, c)
				mess, _ := proto.Marshal(&dto.ConnectMessageResponse{
					Status: dto.ConnectStatus_SUCCESS,
				})
				c.Send(mess)
				slog.Info("registered", "id", c.Id, "remote address", c.Conn.RemoteAddr().String())
			case dto.MessageType_CANDIDATE:
				var candidateMess dto.CandidateMessage
				if err := proto.Unmarshal(message.Data, &candidateMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
					continue
				}
				rc, ok := s.registered.Get(candidateMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", candidateMess.RemoteId)
					continue
				}
				candidateMess.RemoteId = c.Id
				mess, _ := proto.Marshal(&candidateMess)
				rc.Send(mess)
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
			case dto.MessageType_CHAT:
				var chatMess dto.ChatMessage
				if err := proto.Unmarshal(message.Data, &chatMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
					continue
				}
				rc, ok := s.registered.Get(chatMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", chatMess.RemoteId, "local id", c.Id)
					continue
				}
				for _, v := range chatMess.MessageChain {
					if v.Type != dto.MessageUnitType_TEXT &&
						v.Type != dto.MessageUnitType_CALL &&
						v.Type != dto.MessageUnitType_ANSWER &&
						v.Type != dto.MessageUnitType_ESTABLISH {
						slog.Error("ilegal message unit type", "got", v.Type)
						break BEGIN
					}
				}
				chatMess.RemoteId = c.Id
				chatMess.DisplayName = c.DisplayName
				mess, _ := proto.Marshal(&chatMess)
				rc.Send(mess)
				slog.Debug("sent chat message", "id", c.Id, "raw json", mess)
			}
		}
	}
}
