package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/utils"
	"github.com/nsqio/go-nsq"
)

type Producer[MessType any] struct {
	Byter    utils.Byter[MessType]
	Producer *nsq.Producer
	Mq       Mq
}

func (p *Producer[MessType]) Enqueue(message MessType) (chan *nsq.ProducerTransaction, error) {
	doneChan := make(chan *nsq.ProducerTransaction)
	body := p.Byter.ToByte(message)
	if err := p.Producer.PublishAsync(p.Mq.GetTopic(), body, doneChan); err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return doneChan, nil
}

func (p *Producer[MessType]) Close() {
	// Gracefully stop the producer.
	p.Producer.Stop()
}

type ReceiveProducer struct {
	Producer[*frame.Frame]
}

// type SendProducer = Producer[*frame.Frame]
// type BatchFlushProducer = Producer[[]byte]
