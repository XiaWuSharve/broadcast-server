package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/XiaWuSharve/whisperly/utils"
	"github.com/nsqio/go-nsq"
)

type Producer[MessType any] interface {
	Enqueue(message MessType) (chan *nsq.ProducerTransaction, error)
	Close()
}

type ProducerBase[MessType any] struct {
	Byter    utils.Encoder[MessType]
	Producer *nsq.Producer
	Mq       Mq
}

func (p *ProducerBase[MessType]) Enqueue(message MessType) (chan *nsq.ProducerTransaction, error) {
	doneChan := make(chan *nsq.ProducerTransaction)
	body := p.Byter.ToByte(message)
	if err := p.Producer.PublishAsync(p.Mq.GetTopic(), body, doneChan); err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return doneChan, nil
}

func (p *ProducerBase[MessType]) Close() {
	// Gracefully stop the producer.
	p.Producer.Stop()
}

type ReceiveProducer struct {
	ProducerBase[*frame.ReceiveFrame]
}

var _ Producer[*frame.ReceiveFrame] = (*ReceiveProducer)(nil)

type SendProducer struct {
	ProducerBase[*message.Message]
}

var _ Producer[*message.Message] = (*SendProducer)(nil)

// type BatchFlushProducer = Producer[[]byte]
