package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/gorilla/websocket"
)

type WebSocketServer struct {
	ServerBase[*websocket.Conn]
	upgrader *websocket.Upgrader
}

var _ Listener = (*WebSocketServer)(nil)

func NewWebSocketServer(rBufSize int, wBufSize int) *WebSocketServer {
	server := &WebSocketServer{
		ServerBase: ServerBase[*websocket.Conn]{},
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  rBufSize,
			WriteBufferSize: wBufSize,
		},
	}
	server.Listener = server
	return server
}

func (s *WebSocketServer) Listen(ctx context.Context, listener net.Listener) error {
	// 注册 WebSocket 路由
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error("unable to upgrade the HTTP server connection to the WebSocket protocol", "err", err)
			return
		}
		s.CreateConn(ctx, &client.WsConn{Conn: conn, Id: datas.GenId()})
	})

	return http.Serve(listener, nil)
}
