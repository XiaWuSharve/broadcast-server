package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"

	"github.com/nsqio/go-nsq"
)

type Producer[MessType any] interface {
	Enqueue(message MessType) (chan *nsq.ProducerTransaction, error)
	Close()
}

type producerBase[MessType any] struct {
	Byter    datas.Encoder[MessType]
	Producer *nsq.Producer
	Mq       Mq
}

func (p *producerBase[MessType]) Enqueue(message MessType) (chan *nsq.ProducerTransaction, error) {
	doneChan := make(chan *nsq.ProducerTransaction)
	body := p.Byter.ToByte(message)
	if err := p.Producer.PublishAsync(p.Mq.GetTopic(), body, doneChan); err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return doneChan, nil
}

func (p *producerBase[MessType]) Close() {
	// Gracefully stop the producer.
	p.Producer.Stop()
}

type ReceiveProducer struct {
	producerBase[*datas.ReceiveFrame]
}

var _ Producer[*datas.ReceiveFrame] = (*ReceiveProducer)(nil)

type SendProducer struct {
	producerBase[*datas.Message]
}

var _ Producer[*datas.Message] = (*SendProducer)(nil)

// type BatchFlushProducer = Producer[[]byte]
