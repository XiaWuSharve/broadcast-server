package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"

	"github.com/nsqio/go-nsq"
)

type Producer struct {
	Producer *nsq.Producer
	Topic    string
}

func (p *Producer) Enqueue(data datas.Encodable) (chan *nsq.ProducerTransaction, error) {
	doneChan := make(chan *nsq.ProducerTransaction)
	if err := p.Producer.PublishAsync(p.Topic, data.ToByte(), doneChan); err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return doneChan, nil
}

func (p *Producer) Close() {
	// Gracefully stop the producer.
	p.Producer.Stop()
}

// type ReceiveProducer struct {
// 	producerBase
// }

// var _ Producer[*datas.ReceiveFrame] = (*ReceiveProducer)(nil)

// type SendProducer struct {
// 	producerBase[*datas.Message]
// }

// var _ Producer[*datas.Message] = (*SendProducer)(nil)

// type BatchFlushProducer = Producer[[]byte]
