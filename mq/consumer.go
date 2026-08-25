package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/nsqio/go-nsq"
)

type Consumer[MessType any] struct {
	parser   datas.Decoder[MessType]
	Consumer *nsq.Consumer
	Mq       Mq
}

type Handler[MessType any] interface {
	Handle(message MessType) error
}

func (c *Consumer[M]) Start(handler Handler[M]) error {
	h := func(message *nsq.Message) error {
		data, err := c.parser.Parse(message.Body)
		if err != nil {
			return err
		}
		return handler.Handle(data)
	}
	c.Consumer.AddHandler(nsq.HandlerFunc(h))

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err := c.Consumer.ConnectToNSQLookupd(c.Mq.GetNsqLookupdAddress())
	if err != nil {
		return fmt.Errorf("consumer failed to connect to nsq lookup damon: %w", err)
	}
	return nil
}

func (c *Consumer[MessType]) Stop() chan int {
	c.Consumer.Stop()
	return c.Consumer.StopChan
}

type ReceiveConsumer struct {
	Consumer[*datas.Message]
}

// type SendConsumer = Consumer[[]byte]
// type BatchFlushConsumer = Consumer[[]byte]
