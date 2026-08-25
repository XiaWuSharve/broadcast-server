package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/utils"
	"github.com/cespare/xxhash"
	"google.golang.org/protobuf/proto"
)

type KcpServer struct {
	ServerBase[net.Conn, frame.ReceiveFrame]
}

var _ Listener = (*KcpServer)(nil)
var _ DataTransformer[frame.ReceiveFrame] = (*KcpServer)(nil)

func NewKcpServer() *KcpServer {
	server := &KcpServer{
		ServerBase: ServerBase[net.Conn, frame.ReceiveFrame]{
			registered: utils.NewShardMap[string, *client.Client[net.Conn, frame.ReceiveFrame]](config.Cfg.RegistryMaxBucketNum, func(k string) uint64 { return xxhash.Sum64([]byte(k)) }),
		},
	}
	server.DataTransformer = server
	server.Listener = server
	return server
}

func (s *KcpServer) Listen(ctx context.Context, listener net.Listener) error {
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
			session.Close()
		})
		c := client.NewKcpClient(session)
		s.CreateConn(ctx, &c.Client)
	}
}

func (s *KcpServer) TransformRequest(data *frame.ReceiveFrame) (int64, *message.Message, error) {
	var m message.Message
	if err := proto.Unmarshal(data.Payload, &m); err != nil {
		return 0, nil, fmt.Errorf("cannot parse data %s: %w", data.Payload, err)
	}
	return data.CreatedTime, &m, nil
}

func (s *KcpServer) TransformResponse(createdTime int64, mess *message.Message) *frame.ReceiveFrame {
	var frame frame.ReceiveFrame
	messBytes, _ := proto.Marshal(mess)
	frame.Payload = messBytes
	frame.CreatedTime = createdTime
	return &frame
}
