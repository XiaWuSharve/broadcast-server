package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/XiaWuSharve/broadcast-server/dto"
	"github.com/gorilla/websocket"
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
	slog.Info("server started at ws://192.168.239.36:3001/ws")
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
		WriteChan: make(chan []byte),
		ReadChan:  make(chan []byte),
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
	for {
	BEGIN:
		select {
		case <-s.Cancel:
			return fmt.Errorf("canceled")
		case data := <-c.ReadChan:
			var message dto.Message
			if err := json.Unmarshal(data, &message); err != nil {
				slog.Error("cannot parse", "err", err, "data", string(data))
				continue
			}

			switch message.Type {
			case dto.CONNECT:
				var id dto.ConnectMessage
				if err := json.Unmarshal(message.Data, &id); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
					continue
				}
				c.Id = id
				s.registered.Set(c.Id, c)
			case dto.CANDIDATE:
				var candidateMess dto.CandidateMessage
				if err := json.Unmarshal(message.Data, &candidateMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
					continue
				}
				rc, ok := s.registered.Get(candidateMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", candidateMess.RemoteId)
					continue
				}
				data, _ := json.Marshal(candidateMess)
				message.Data = data
				mess, _ := json.Marshal(message)
				rc.Send(mess)
			case dto.CHAT:
				var chatMess dto.ChatMessage
				if err := json.Unmarshal(message.Data, &chatMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
					continue
				}
				if _, ok := s.registered.Get(chatMess.LocalId); !ok {
					s.registered.Set(c.Id, c)
				}
				rc, ok := s.registered.Get(chatMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", chatMess.RemoteId, "local id", chatMess.LocalId)
					continue
				}
				for _, v := range chatMess.MessageChain {
					if v.Type != dto.TEXT && v.Type != dto.CALL && v.Type != dto.ANSWER && v.Type != dto.ESTABLISH {
						slog.Error("ilegal message unit type", "got", v.Type)
						break BEGIN
					}
				}
				data, _ := json.Marshal(chatMess)
				message.Data = data
				mess, _ := json.Marshal(message)
				rc.Send(mess)
			}
		}
	}
}
