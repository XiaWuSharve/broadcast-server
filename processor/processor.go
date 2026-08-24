package processor

import (
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/utils"
)

type Processor[ConnType any] struct {
	consumer *mq.ReceiveConsumer
	sender   *mq.SendProducer
	// registry  *registry.UserRepo
	converter utils.Converter[*frame.ReceiveFrame, *message.Message]
}

var _ mq.Handler[*frame.ReceiveFrame] = (*Processor[struct{}])(nil)

func New[ConnType any](consumer *mq.ReceiveConsumer, producer *mq.SendProducer) *Processor[ConnType] {
	return &Processor[ConnType]{consumer, producer, &utils.Frame2Message{}}
}

// TODO 抽出发送入队逻辑
func (p *Processor[C]) Handle(frame *frame.ReceiveFrame) error {
	slog.Debug("received raw message", "payload", frame.Payload)
	m, err := p.converter.Convert(frame)
	slog.Debug("message", "mess", m.String())
	if err != nil {
		// FAIL message
		m.Data = &message.Message_Ack{Ack: &message.Ack{
			MessageId: m.MessageId,
			Status:    message.AckStatus_FAIL,
			Reason:    fmt.Sprintf("请求格式错误: %s", err),
		}}
		m.SenderId = m.ReceiverId
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
		return nil
	}
	switch m.GetData().(type) {
	case *message.Message_Candidate:
		// TODO check candiMess info
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	case *message.Message_Chat:
		chatMess := m.GetChat()
		for _, v := range chatMess.MessageChain {
			if v.Type != message.MessageUnitType_TEXT &&
				v.Type != message.MessageUnitType_CALL &&
				v.Type != message.MessageUnitType_ANSWER &&
				v.Type != message.MessageUnitType_ESTABLISH {
				return fmt.Errorf("ilegal message unit type: %s", v.Type.String())
			}
		}
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	case *message.Message_Call:
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	case *message.Message_Answer:
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	}
	m.Data = &message.Message_Ack{Ack: &message.Ack{
		MessageId: m.MessageId,
		Status:    message.AckStatus_SENT,
	}}
	m.SenderId = m.ReceiverId
	_, err = p.sender.Enqueue(m)
	if err != nil {
		return fmt.Errorf("cannot enqueue sender: %w", err)
	}
	return nil
}

// func ToWriteChan() {
// 	rc.WriteChan <- s.TransformResponse(createdTime, m)
// 	slog.Debug("sent chat message", "id", c.Id, "raw json", m)
// }
