package mq

import (
	"fmt"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/nsqio/go-nsq"
	"google.golang.org/protobuf/proto"
)

type Mq[MessType any] struct {
	MqInterface[MessType]
	config  *nsq.Config
	Address string
	Topic   string
}

type MqInterface[MessType any] interface {
	// returns: emitFunc, closeFunc, error
	CreateProducer() (func(message MessType) (chan *nsq.ProducerTransaction, error), func(), error)
	// returns: closeFunc, error
	CreateConsumer(handler func(message MessType) error) (func() chan int, error)
	// subclass
	TransformRequest(data MessType) []byte
	TransformResponse(data []byte) MessType
}

var _ MqInterface[struct{}] = (*Mq[struct{}])(nil)

func (mq *Mq[M]) CreateProducer() (func(message M) (chan *nsq.ProducerTransaction, error), func(), error) {
	producer, err := nsq.NewProducer(mq.Address, mq.config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create producer: %w", err)
	}
	doneChan := make(chan *nsq.ProducerTransaction)
	enqueue := func(message M) (chan *nsq.ProducerTransaction, error) {
		body := mq.TransformRequest(message)
		err := producer.PublishAsync("test", body, doneChan)
		if err != nil {
			return nil, fmt.Errorf("failed to publish message %s: %w", mq.TransformRequest(message), err)
		}
		return doneChan, nil
	}
	close := func() {
		// Gracefully stop the producer.
		producer.Stop()
	}
	return enqueue, close, nil
}

func (mq *Mq[M]) CreateConsumer(handler func(message M) error) (func() chan int, error) {
	consumer, err := nsq.NewConsumer(mq.Topic, "processor", mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	h := func(message *nsq.Message) error {
		return handler(mq.TransformResponse(message.Body))
	}
	consumer.AddHandler(nsq.HandlerFunc(h))

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err = consumer.ConnectToNSQLookupd(mq.Address)
	if err != nil {
		return nil, fmt.Errorf("consumer failed to connect to nsq lookup damon: %w", err)
	}

	stop := func() chan int {
		consumer.Stop()
		return consumer.StopChan
	}
	return stop, nil
}

type InputMq Mq[*message.Message]

func (s *InputMq) TransformRequest(mess *message.Message) []byte {
	if mess == nil {
		return []byte{}
	}
	bytes, _ := proto.Marshal(mess)
	return bytes
}

func (s *InputMq) TransformResponse(data []byte) *message.Message {
	var m message.Message
	if proto.Unmarshal(data, &m) != nil {
		return nil
	}
	return &m
}

type OutputMq Mq[[]byte]

func (s *OutputMq) TransformRequest(data []byte) []byte {
	return data
}

func (s *OutputMq) TransformResponse(data []byte) []byte {
	return data
}

type BatchFlushMq OutputMq

func NewMq[MessType any](address string, topic string) (*Mq[MessType], error) {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	im := &Mq[MessType]{
		config:  config,
		Address: address,
		Topic:   topic,
	}
	// im.MqInterface = im

	return im, nil
}

var NewInputMq = func(address string) (*InputMq, error) {
	mq, err := NewMq[*message.Message](address, "input_message")
	inputMq := (*InputMq)(mq)
	// inputMq.MqInterface = inputMq
	return inputMq, err
}
var NewOutputMq = func(address string) (*OutputMq, error) {
	outMq, err := NewMq[[]byte](address, "output_message")
	return (*OutputMq)(outMq), err
}
var NewBatchFlusherMq = func(address string) (*BatchFlushMq, error) {
	batchFlushMq, err := NewMq[[]byte](address, "message_persistence")
	return (*BatchFlushMq)(batchFlushMq), err
}
