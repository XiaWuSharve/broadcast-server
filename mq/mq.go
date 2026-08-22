package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/dto/message"
	"github.com/nsqio/go-nsq"
	"google.golang.org/protobuf/proto"
)

type MqBase[I, O any] struct {
	DataTransformer[I, O]
	config            *nsq.Config
	Topic             string
	NsqLookupdAddress string
}

type Mq[I, O any] interface {
	CreateProducer(nsqdAddress string) (*Producer[I], error)
	CreateConsumer(nsqLookupdAddress string) (*Consumer[O], error)
}

type DataTransformer[I, O any] interface {
	TransformRequest(data I) ([]byte, error)
	TransformResponse(data []byte) (O, error)
}

var _ Mq[struct{}, struct{}] = (*MqBase[struct{}, struct{}])(nil)

func (mq *MqBase[I, O]) CreateProducer(nsqdAddress string) (*Producer[I], error) {
	producer, err := nsq.NewProducer(nsqdAddress, mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Producer[I]{
		Producer: producer,
		Mq:       mq,
	}, nil
}

func (mq *MqBase[I, O]) CreateConsumer(nsqLookupdAddress string) (*Consumer[O], error) {
	consumer, err := nsq.NewConsumer(mq.Topic, "processor", mq.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Consumer[O]{
		Consumer: consumer,
		Mq:       mq,
	}, nil
}

type ReceiverDataTransformer struct{}

var _ DataTransformer[[]byte, *message.Message] = (*ReceiverDataTransformer)(nil)

func (s *ReceiverDataTransformer) TransformRequest(data []byte) ([]byte, error) {
	return data, nil
}

func (s *ReceiverDataTransformer) TransformResponse(data []byte) (*message.Message, error) {
	var m message.Message
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

type SenderDataTransformer struct{}

var _ DataTransformer[*message.Message, []byte] = (*SenderDataTransformer)(nil)

func (s *SenderDataTransformer) TransformRequest(data *message.Message) ([]byte, error) {
	return proto.Marshal(data)
}

func (s *SenderDataTransformer) TransformResponse(data []byte) ([]byte, error) {
	return data, nil
}

type BatchFlushDataTransformer struct{}

var _ DataTransformer[[]byte, []byte] = (*BatchFlushDataTransformer)(nil)

func (s *BatchFlushDataTransformer) TransformRequest(data []byte) ([]byte, error) {
	return data, nil
}

func (s *BatchFlushDataTransformer) TransformResponse(data []byte) ([]byte, error) {
	return data, nil
}

func NewBaseMq[I, O any](topic string, dataTransformer DataTransformer[I, O]) (*MqBase[I, O], error) {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	im := &MqBase[I, O]{
		config: config,
		Topic:  topic,
	}
	im.DataTransformer = dataTransformer

	return im, nil
}

type RecieveMq = MqBase[[]byte, *message.Message]
type SendMq = MqBase[*message.Message, []byte]
type BatchFlushMq = MqBase[[]byte, []byte]

func NewRecieveMq() (*RecieveMq, error) {
	mq, err := NewBaseMq("input_message", &ReceiverDataTransformer{})
	return (*RecieveMq)(mq), err
}
func NewSendMq() (*SendMq, error) {
	outMq, err := NewBaseMq("output_message", &SenderDataTransformer{})
	return (*SendMq)(outMq), err
}
func NewBatchFlushMq() (*BatchFlushMq, error) {
	batchFlushMq, err := NewBaseMq("message_persistence", &BatchFlushDataTransformer{})
	return (*BatchFlushMq)(batchFlushMq), err
}
