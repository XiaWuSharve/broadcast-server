package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/nsqio/go-nsq"
)

type Producer[MessType any] struct {
	Producer *nsq.Producer
	Mq       *MqBase[MessType, any]
}

func (p *Producer[MessType]) Enqueue(message MessType) (chan *nsq.ProducerTransaction, error) {
	doneChan := make(chan *nsq.ProducerTransaction)
	body, err := p.Mq.TransformRequest(message)
	if err != nil {
		return nil, fmt.Errorf("cannot transform request: %w", err)
	}
	err = p.Producer.PublishAsync(p.Mq.Topic, body, doneChan)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return doneChan, nil
}

func (p *Producer[MessType]) Close() {
	// Gracefully stop the producer.
	p.Producer.Stop()
}

type ReceiveProducer = Producer[*message.Message]
type SendProducer = Producer[*message.Message]
type BatchFlushProducer = Producer[[]byte]
