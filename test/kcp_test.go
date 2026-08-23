package test

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"testing"
	"time"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/server"
	"github.com/xtaci/kcp-go/v5"
	"google.golang.org/protobuf/proto"
)

func TestKCP(t *testing.T) {
	listener, err := kcp.Listen("0.0.0.0:3001")
	if err != nil {
		t.Error(err)
	}

	s := server.NewKcpServer()
	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancelTimeout := context.WithTimeout(ctx, 3*time.Second)
	go func() {
		if err := s.Start(ctx, listener); err != listener.Close() {
			t.Log(err)
		}
		t.Log("shutdown")
	}()
	defer listener.Close()
	time.Sleep(time.Second)
	sess, err := kcp.Dial("127.0.0.1:3001")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer sess.Close()

	c := client.NewKcpClient(sess)

	payload, _ := proto.Marshal(&message.Message{
		Data: &message.Message_ConnectMessageRequest{
			ConnectMessageRequest: &message.ConnectMessageRequest{
				Id:          "sharve",
				DisplayName: "夏午",
			},
		},
	})
	c.Send(&frame.Frame{
		CreatedTime: time.Now().UnixMilli(),
		Payload:     payload,
	})

	f, err := c.Receive()
	if err != nil {
		t.Error(err)
	}

	var m message.Message
	if err := proto.Unmarshal(f.Payload, &m); err != nil {
		t.Error(err)
	}

	if m.GetConnectMessageResponse().GetStatus() != message.ConnectStatus_SUCCESS {
		t.Error("failed")
	}

	time.Sleep(time.Second)

	dto := &message.Message{
		Data: &message.Message_ChatMessage{ChatMessage: &message.ChatMessage{
			RemoteId: "sharve",
			MessageChain: []*message.MessageUnit{
				{
					Type:    message.MessageUnitType_TEXT,
					Message: "hello kcp+protobuf",
				},
			},
		}},
	}

	payload, _ = proto.Marshal(dto)
	jsonPayload, _ := json.Marshal(dto)

	c.Send(&frame.Frame{
		CreatedTime: time.Now().UnixMilli(),
		Payload:     payload,
	})

	f, err = c.Receive()
	if err != nil {
		t.Error(err)
	}
	if err := proto.Unmarshal(f.Payload, &m); err != nil {
		t.Error(err)
	}

	if m.GetChatMessage().GetDisplayName() != "夏午" || m.GetChatMessage().GetMessageChain()[0].Message != "hello kcp+protobuf" {
		t.Errorf("failed received: %s %s", m.GetChatMessage().GetDisplayName(), m.GetChatMessage().GetMessageChain()[0].Message)
	}

	slog.Info("compression rate (byte)", "before", len(jsonPayload), "after", len(payload)+12)
	cancelTimeout()
	cancel()
}
