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

type MqBase[I, O any] struct {
	config            *nsq.Config
	Topic             string
	NsqLookupdAddress string
}

var _ Mq = (*MqBase[any, any])(nil)

func (mq *MqBase[I, O]) GetTopic() string {
	return mq.Topic
}

func (mq *MqBase[I, O]) GetNsqLookupdAddress() string {
	return mq.NsqLookupdAddress
}

func (mq *MqBase[I, O]) CreateProducer(nsqdAddress string, byter datas.Encoder[I]) (*ProducerBase[I], error) {
	producer, err := nsq.NewProducer(nsqdAddress, mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &ProducerBase[I]{
		Producer: producer,
		Mq:       mq,
		Byter:    byter,
	}, nil
}

func (mq *MqBase[I, O]) CreateConsumer(nsqLookupdAddress string, parser datas.Decoder[O]) (*Consumer[O], error) {
	consumer, err := nsq.NewConsumer(mq.Topic, "processor", mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Consumer[O]{
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

func newBaseMq[I, O any](topic string) (*MqBase[I, O], error) {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	im := &MqBase[I, O]{
		config: config,
		Topic:  topic,
	}
	return im, nil
}

type ReceiveMq struct {
	MqBase[*datas.ReceiveFrame, *datas.Message]
}

func (rmq *ReceiveMq) CreateProducer(nsqLookupdAddress string) (*ReceiveProducer, error) {
	baseProducer, err := rmq.MqBase.CreateProducer(nsqLookupdAddress, &datas.FrameByter{})
	if err != nil {
		return nil, err
	}
	return &ReceiveProducer{ProducerBase: *baseProducer}, nil
}

func (rmq *ReceiveMq) CreateConsumer(nsqLookupdAddress string) (*ReceiveConsumer, error) {
	baseConsumer, err := rmq.MqBase.CreateConsumer(nsqLookupdAddress, &datas.MessageParser{})
	if err != nil {
		return nil, err
	}
	return &ReceiveConsumer{
		Consumer: *baseConsumer,
	}, nil
}

func NewRecieveMq() (*ReceiveMq, error) {
	mq, err := newBaseMq[*datas.ReceiveFrame, *datas.Message]("input_message")
	if err != nil {
		return nil, err
	}
	return &ReceiveMq{*mq}, nil
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
