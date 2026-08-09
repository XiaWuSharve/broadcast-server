package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/XiaWuSharve/kcp-webrtc-server/client"
	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/XiaWuSharve/kcp-webrtc-server/utils"
	"github.com/cespare/xxhash"
	"github.com/gorilla/websocket"
)

type WebSocketServer struct {
	ServerBase[*websocket.Conn, message.Message]
	upgrader *websocket.Upgrader
}

var _ Server[message.Message] = (*WebSocketServer)(nil)

func NewWebSocketServer(rBufSize int, wBufSize int) *WebSocketServer {
	server := &WebSocketServer{
		ServerBase: ServerBase[*websocket.Conn, message.Message]{
			// TODO config file
			registered: utils.NewShardMap[string, *client.Client[*websocket.Conn, message.Message]](128, func(k string) uint64 { return xxhash.Sum64([]byte(k)) }),
		},
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  rBufSize,
			WriteBufferSize: wBufSize,
		},
	}
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
		c := client.NewWebSocketClient(conn)
		s.CreateConn(ctx, &c.Client)
	})

	// 使用传入的 listener 启动服务
	slog.Info("server started", "addr", listener.Addr().String())
	return http.Serve(listener, nil)
}

func (s *WebSocketServer) TransformRequest(data *message.Message) (int64, *message.Message, error) {
	return data.CreatedTime, data, nil
}

func (s *WebSocketServer) TransformResponse(createdTime int64, mess *message.Message) *message.Message {
	mess.CreatedTime = createdTime
	return mess
}
