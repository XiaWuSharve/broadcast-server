package test

import (
	"log/slog"
	"testing"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/XiaWuSharve/kcp-webrtc-server/mq"
)

func TestMq(t *testing.T) {
	inputMq, err := mq.NewInputMq()
	if err != nil {
		t.Fatal("cannot create input mq", err)
	}
	emit, closeProducer, err := inputMq.CreateProducer("localhost:4150")
	if err != nil {
		t.Fatal("failed to create Producer", err)
	}
	defer closeProducer()
	transactionChan, err := emit(&message.Message{
		Data: &message.Message_ChatMessage{
			ChatMessage: &message.ChatMessage{
				RemoteId: "sharve",
				MessageChain: []*message.MessageUnit{
					{
						Type:    message.MessageUnitType_TEXT,
						Message: "hello mq",
					},
				},
			},
		},
	})
	transaction := <-transactionChan
	if err := transaction.Error; err != nil {
		t.Fatal("emit failed", err)
	}
	doneChan := make(chan struct{})
	handleFunc := func(m *message.Message) error {
		if m == nil {
			t.Fatal("impossible nil pointer")
		}
		slog.Info("processing", "data", m.String())
		if m.GetChatMessage().GetRemoteId() != "sharve" || m.GetChatMessage().GetMessageChain()[0].Message != "hello mq" {
			t.Errorf("failed received: %s %s", m.GetChatMessage().GetRemoteId(), m.GetChatMessage().GetMessageChain()[0].Message)
		}
		close(doneChan)
		return nil
	}
	closeConsumer, err := inputMq.CreateConsumer("localhost:4161", handleFunc)
	defer func() {
		<-closeConsumer()
		slog.Info("consumer closed")
	}()
	<-doneChan
}
