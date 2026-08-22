package processor

import (
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/registry"
)

type Processor[ConnType any] struct {
	consumer *mq.ReceiveConsumer
	sender   *mq.SendProducer
	registry *registry.Registry[ConnType, *message.Message]
}

var _ mq.Handler[*message.Message] = (*Processor[struct{}])(nil)

func New[ConnType any](consumer *mq.ReceiveConsumer, producer *mq.SendProducer, registry *registry.Registry[ConnType, *message.Message]) *Processor[ConnType] {
	return &Processor[ConnType]{consumer, producer, registry}
}

func (p *Processor[C]) Handle(m *message.Message) error {
	slog.Debug("received raw message", "mess", m.String())
	switch m.GetData().(type) {
	case *message.Message_CandidateMessage:
		// TODO check candiMess info
		p.sender.Enqueue(m)
	case *message.Message_ChatMessage:
		chatMess := m.GetChatMessage()
		for _, v := range chatMess.MessageChain {
			if v.Type != message.MessageUnitType_TEXT &&
				v.Type != message.MessageUnitType_CALL &&
				v.Type != message.MessageUnitType_ANSWER &&
				v.Type != message.MessageUnitType_ESTABLISH {
				return fmt.Errorf("ilegal message unit type: %s", v.Type.String())
			}
		}
		c, _ := p.registry.Get(m.SenderId)
		chatMess.DisplayName = c.DisplayName
		p.sender.Enqueue(m)
	case *message.Message_CallSdp:
		p.sender.Enqueue(m)
	case *message.Message_AnswerSdp:
		p.sender.Enqueue(m)
	}
	return nil
}

// func ToWriteChan() {
// 	rc.WriteChan <- s.TransformResponse(createdTime, m)
// 	slog.Debug("sent chat message", "id", c.Id, "raw json", m)
// }
