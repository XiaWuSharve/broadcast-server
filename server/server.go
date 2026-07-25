package server

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/XiaWuSharve/kcp-webrtc-server/utils"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	upgrader   *websocket.Upgrader
	registered *utils.ConcurrentMap[string, *Client]
	Cancel     chan struct{}
}

func New(rBufSize int, wBufSize int) *Server {
	server := &Server{
		registered: utils.NewConcurrentMap[string, *Client](),
		Cancel:     make(chan struct{}),
	}
	server.setUpgrader(rBufSize, wBufSize)
	return server
}

func (s *Server) Start(listener net.Listener) error {
	// 注册 WebSocket 路由
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("unable to upgrade the HTTP server connection to the WebSocket protocol", "err", err)
			return
		}
		if err := s.CreateConn(conn); err != nil {
			slog.Error("cannot create conn", "err", err)
		}
	})

	// 使用传入的 listener 启动服务
	slog.Info("server started", "addr", listener.Addr().String())
	return http.Serve(listener, nil)
}

func (s *Server) setUpgrader(rBufSize int, wBufSize int) {
	s.upgrader = &websocket.Upgrader{
		ReadBufferSize:  rBufSize,
		WriteBufferSize: wBufSize,
	}
}

func (s *Server) CreateConn(session *websocket.Conn) error {

	c := &Client{
		Conn:      session,
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
	slog.Info("started to handle an anonymous client", "remote address", c.Conn.RemoteAddr().String())
	for {
	BEGIN:
		select {
		case <-s.Cancel:
			return fmt.Errorf("canceled")
		case data := <-c.ReadChan:
			slog.Debug("received", "raw message", string(data))
			var m message.Message
			if err := proto.Unmarshal(data, &m); err != nil {
				slog.Error("cannot parse", "err", err, "data", string(data))
				continue
			}
			timestamp := m.GetCreatedTime()
			now := time.Now().UnixMilli()
			diff := timestamp - now
			if diff < 0 {
				diff = -diff
			}
			if diff > 5*60*1000 { // 5分钟对应的毫秒数
				slog.Error("client time too slow or fast", "client time", time.UnixMilli(timestamp).Format(time.DateTime))
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
				mess, _ := proto.Marshal(&message.ConnectMessageResponse{
					Status: message.ConnectStatus_SUCCESS,
				})
				c.WriteChan <- mess
				slog.Info("registered", "id", c.Id, "remote address", c.Conn.RemoteAddr().String())
			case *message.Message_CandidateMessage:
				candiMess := m.GetCandidateMessage()
				rc, ok := s.registered.Get(candiMess.GetRemoteId())
				if !ok {
					slog.Error("remote id not found", "remote id", candiMess.GetRemoteId())
					continue
				}
				candiMess.RemoteId = c.Id
				mess, _ := proto.Marshal(candiMess)
				rc.WriteChan <- mess
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
				mess, _ := proto.Marshal(chatMess)
				rc.WriteChan <- mess
				slog.Debug("sent chat message", "id", c.Id, "raw json", mess)
			}
		}
	}
}
