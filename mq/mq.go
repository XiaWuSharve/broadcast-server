package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/nsqio/go-nsq"
	"google.golang.org/protobuf/proto"
)

type Mq interface {
	GetTopic() string
	GetNsqLookupdAddress() string
}

type ProducerConsumerCreater[P, C any] interface {
	CreateProducer(nsqdAddress string, byter datas.Encoder[P]) (*P, error)
	CreateConsumer(nsqLookupdAddress string, parser datas.Decoder[C]) (*C, error)
}

type mqBase[T any] struct {
	config            *nsq.Config
	Topic             string
	NsqLookupdAddress string
}

var _ Mq = (*mqBase[any])(nil)

func (mq *mqBase[T]) GetTopic() string {
	return mq.Topic
}

func (mq *mqBase[T]) GetNsqLookupdAddress() string {
	return mq.NsqLookupdAddress
}

func (mq *mqBase[T]) CreateProducer(nsqdAddress string, byter datas.Encoder[T]) (*producerBase[T], error) {
	producer, err := nsq.NewProducer(nsqdAddress, mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &producerBase[T]{
		Producer: producer,
		Mq:       mq,
		Byter:    byter,
	}, nil
}

func (mq *mqBase[T]) CreateConsumer(nsqLookupdAddress string, parser datas.Decoder[T]) (*Consumer[T], error) {
	consumer, err := nsq.NewConsumer(mq.Topic, "processor", mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Consumer[T]{
		Consumer: consumer,
		Mq:       mq,
		parser:   parser,
	}, nil
}

type ReceiverDataTransformer struct{}

func (s *ReceiverDataTransformer) TransformRequest(data []byte) ([]byte, error) {
	return data, nil
}

func (s *ReceiverDataTransformer) TransformResponse(dat []byte) (*datas.Message, error) {
	var m datas.Message
	if err := proto.Unmarshal(dat, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func newMqBase[T any](topic string) (*mqBase[T], error) {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	im := &mqBase[T]{
		config: config,
		Topic:  topic,
	}
	return im, nil
}

type ReceiveMq struct {
	mqBase[*datas.ReceiveFrame]
}

func (rmq *ReceiveMq) CreateProducer(nsqLookupdAddress string) (*ReceiveProducer, error) {
	baseProducer, err := rmq.mqBase.CreateProducer(nsqLookupdAddress, &datas.ReceiveFrameEncoder{})
	if err != nil {
		return nil, err
	}
	return &ReceiveProducer{producerBase: *baseProducer}, nil
}

func (rmq *ReceiveMq) CreateConsumer(nsqLookupdAddress string) (*ReceiveConsumer, error) {
	baseConsumer, err := rmq.mqBase.CreateConsumer(nsqLookupdAddress, &datas.ReceiveFrameParser{})
	if err != nil {
		return nil, err
	}
	return &ReceiveConsumer{
		Consumer: *baseConsumer,
	}, nil
}

func NewRecieveMq() (*ReceiveMq, error) {
	mq, err := newMqBase[*datas.ReceiveFrame]("input_message")
	if err != nil {
		return nil, err
	}
	return &ReceiveMq{*mq}, nil
}

type SendMq struct {
	mqBase[*datas.SendFrame]
}

func (rmq *SendMq) CreateProducer(nsqLookupdAddress string) (*ReceiveProducer, error) {
	baseProducer, err := rmq.mqBase.CreateProducer(nsqLookupdAddress, &datas.MessageEncoder{})
	if err != nil {
		return nil, err
	}
	return &ReceiveProducer{producerBase: *baseProducer}, nil
}

func (rmq *SendMq) CreateConsumer(nsqLookupdAddress string) (*ReceiveConsumer, error) {
	baseConsumer, err := rmq.mqBase.CreateConsumer(nsqLookupdAddress, &datas.MessageParser{})
	if err != nil {
		return nil, err
	}
	return &ReceiveConsumer{
		Consumer: *baseConsumer,
	}, nil
}

func NewSendMq() (*SendMq, error) {
	mq, err := newMqBase[*datas.ReceiveFrame, *datas.Message]("input_message")
	if err != nil {
		return nil, err
	}
	return &SendMq{*mq}, nil
}

// type SendMq struct {
// 	MqBase[*frame.Frame, []byte]
// }
// type BatchFlushMq = MqBase[[]byte, []byte]

// func NewSendMq() (*SendMq, error) {
// 	outMq, err := NewBaseMq("output_message", &SenderDataTransformer{})
// 	return (*SendMq)(outMq), err
// }
// func NewBatchFlushMq() (*BatchFlushMq, error) {
// 	batchFlushMq, err := NewBaseMq("message_persistence", &BatchFlushDataTransformer{})
// 	return (*BatchFlushMq)(batchFlushMq), err
// }
