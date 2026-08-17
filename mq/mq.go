package mq

import (
	"fmt"

	"github.com/XiaWuSharve/kcp-webrtc-server/dto/message"
	"github.com/nsqio/go-nsq"
	"google.golang.org/protobuf/proto"
)

type Mq[MessType any] struct {
	DataTransformer[MessType]
	config *nsq.Config
	Topic  string
}

type MqInterface[MessType any] interface {
	// returns: emitFunc, closeFunc, error
	CreateProducer(nsqdAddress string) (func(message MessType) (chan *nsq.ProducerTransaction, error), func(), error)
	// returns: closeFunc, error
	CreateConsumer(nsqLookupdAddress string, handler func(message MessType) error) (func() chan int, error)
}

type DataTransformer[MessType any] interface {
	// subclass
	TransformRequest(data MessType) []byte
	TransformResponse(data []byte) MessType
}

var _ MqInterface[struct{}] = (*Mq[struct{}])(nil)

func (mq *Mq[M]) CreateProducer(nsqdAddress string) (func(message M) (chan *nsq.ProducerTransaction, error), func(), error) {
	producer, err := nsq.NewProducer(nsqdAddress, mq.config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create producer: %w", err)
	}
	enqueue := func(message M) (chan *nsq.ProducerTransaction, error) {
		doneChan := make(chan *nsq.ProducerTransaction)
		body := mq.TransformRequest(message)
		err := producer.PublishAsync(mq.Topic, body, doneChan)
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

func (mq *Mq[M]) CreateConsumer(nsqLookupdAddress string, handler func(message M) error) (func() chan int, error) {
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
	err = consumer.ConnectToNSQLookupd(nsqLookupdAddress)
	if err != nil {
		return nil, fmt.Errorf("consumer failed to connect to nsq lookup damon: %w", err)
	}

	stop := func() chan int {
		consumer.Stop()
		return consumer.StopChan
	}
	return stop, nil
}

type MessageTransformer Mq[*message.Message]

var _ DataTransformer[*message.Message] = (*MessageTransformer)(nil)

func (s *MessageTransformer) TransformRequest(mess *message.Message) []byte {
	if mess == nil {
		return []byte{}
	}
	bytes, _ := proto.Marshal(mess)
	return bytes
}

func (s *MessageTransformer) TransformResponse(data []byte) *message.Message {
	var m message.Message
	if proto.Unmarshal(data, &m) != nil {
		return nil
	}
	return &m
}

type ByteTransformer Mq[[]byte]

var _ DataTransformer[[]byte] = (*ByteTransformer)(nil)

func (s *ByteTransformer) TransformRequest(data []byte) []byte {
	return data
}

func (s *ByteTransformer) TransformResponse(data []byte) []byte {
	return data
}

func NewMq[MessType any](topic string, dataTransformer DataTransformer[MessType]) (*Mq[MessType], error) {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	im := &Mq[MessType]{
		config: config,
		Topic:  topic,
	}
	im.DataTransformer = dataTransformer

	return im, nil
}

var NewInputMq = func() (*Mq[*message.Message], error) {
	mq, err := NewMq("input_message", &MessageTransformer{})
	return mq, err
}
var NewOutputMq = func() (*Mq[[]byte], error) {
	outMq, err := NewMq("output_message", &ByteTransformer{})
	return outMq, err
}
var NewBatchFlusherMq = func() (*Mq[[]byte], error) {
	batchFlushMq, err := NewMq("message_persistence", &ByteTransformer{})
	return batchFlushMq, err
}
