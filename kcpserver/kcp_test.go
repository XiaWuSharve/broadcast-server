package kcpserver

import (
	"log"
	"testing"
	"time"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/frame"
	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/xtaci/kcp-go/v5"
	"google.golang.org/protobuf/proto"
)

func TestKCP(t *testing.T) {
	listener, err := kcp.Listen("0.0.0.0:3001")
	if err != nil {
		t.Error(err)
	}

	s := New()
	go func() {
		if err := s.Start(listener); err != listener.Close() {
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

	c := NewClient(sess)

	payload, _ := proto.Marshal(&message.Message{
		Data: &message.Message_ConnectMessageRequest{
			ConnectMessageRequest: &message.ConnectMessageRequest{
				Id:          "sharve",
				DisplayName: "夏午",
			},
		},
	})
	c.Send(&frame.Message{
		CreatedTime: time.Now().UnixMilli(),
		Len:         uint32(len(payload)),
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

	payload, _ = proto.Marshal(&message.Message{
		Data: &message.Message_ChatMessage{ChatMessage: &message.ChatMessage{
			RemoteId: "sharve",
			MessageChain: []*message.MessageUnit{
				{
					Type:    message.MessageUnitType_TEXT,
					Message: "hello kcp+protobuf",
				},
			},
		}},
	})

	c.Send(&frame.Message{
		CreatedTime: time.Now().UnixMilli(),
		Len:         uint32(len(payload)),
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
}
