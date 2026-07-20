package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/XiaWuSharve/broadcast-server/dto"
	"github.com/XiaWuSharve/broadcast-server/dto/request"
	"github.com/XiaWuSharve/broadcast-server/dto/response"
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
		select {
		case <-s.Cancel:
			return fmt.Errorf("canceled")
		case data := <-c.ReadChan:
			var message dto.Message
			if err := json.Unmarshal(data, &message); err != nil {
				slog.Error("cannot parse", "err", err, "data", string(data))
			}

			switch message.Type {
			case dto.CONNECT:
				var id request.ConnectMessage
				if err := json.Unmarshal(message.Data, &id); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
				}
				c.Id = id
				s.registered.Set(c.Id, c)
			case dto.CALL:
				var callMess request.CallMessage
				if err := json.Unmarshal(message.Data, &callMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
				}
				if _, ok := s.registered.Get(callMess.LocalId); !ok {
					s.registered.Set(c.Id, c)
				}
				rc, ok := s.registered.Get(callMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", callMess.RemoteId, "local id", callMess.LocalId)
				}
				var callRsp = response.CallMessage{
					Sdp:      callMess.Sdp,
					RemoteId: callMess.LocalId,
				}
				data, _ := json.Marshal(callRsp)
				message.Data = data
				mess, _ := json.Marshal(message)
				rc.Send(mess)
			case dto.ANSWER:
				var answerMess request.AnswerMessage
				if err := json.Unmarshal(message.Data, &answerMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
				}
				rc, ok := s.registered.Get(answerMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", answerMess.RemoteId)
				}
				var ansRsp response.AnswerMessage = answerMess.Sdp
				message.Data = json.RawMessage(ansRsp)
				mess, _ := json.Marshal(message)
				rc.Send(mess)
			case dto.CANDIDATE:
				var candidateMess request.CandidateMessage
				if err := json.Unmarshal(message.Data, &candidateMess); err != nil {
					slog.Error("cannot parse", "err", err, "data", string(data))
				}
				rc, ok := s.registered.Get(candidateMess.RemoteId)
				if !ok {
					slog.Error("remote id not found", "remote id", candidateMess.RemoteId)
				}
				var candRsp = response.CandidateMessage{
					SdpMid:        candidateMess.SdpMid,
					SdpMLineIndex: candidateMess.SdpMLineIndex,
					Sdp:           candidateMess.Sdp,
				}
				data, _ := json.Marshal(candRsp)
				message.Data = data
				mess, _ := json.Marshal(message)
				rc.Send(mess)
			}
		}
	}
}
