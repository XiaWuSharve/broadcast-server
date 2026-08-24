package utils

import (
	"encoding/binary"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"google.golang.org/protobuf/proto"
)

type Encoder[MessType any] interface {
	ToByte(data MessType) []byte
}

type Converter[S, D any] interface {
	Convert(source S) (D, error)
}

type Decoder[MessType any] interface {
	Parse(data []byte) (MessType, error)
}

type FrameByter struct {
	Buf [8]byte
}

var _ Encoder[*frame.ReceiveFrame] = (*FrameByter)(nil)

func (p *FrameByter) ToByte(data *frame.ReceiveFrame) []byte {
	binary.BigEndian.PutUint64(p.Buf[:], uint64(data.CreatedTime))
	return append(p.Buf[:], data.Payload...)
}

type MessageParser struct {
	CreatedTime int64
	Message     message.Message
}

var _ Decoder[*message.Message] = (*MessageParser)(nil)

func (mp *MessageParser) Parse(data []byte) (*message.Message, error) {
	mp.CreatedTime = int64(binary.BigEndian.Uint64(data[0:8]))
	if err := proto.Unmarshal(data[8:], &mp.Message); err != nil {
		return nil, err
	}
	mp.Message.CreatedTime = mp.CreatedTime
	return &mp.Message, nil
}

type Frame2Message struct {
	Message message.Message
}

var _ Converter[*frame.ReceiveFrame, *message.Message] = (*Frame2Message)(nil)

func (f2m *Frame2Message) Convert(frame *frame.ReceiveFrame) (*message.Message, error) {
	if err := proto.Unmarshal(frame.Payload, &f2m.Message); err != nil {
		return nil, err
	}
	f2m.Message.CreatedTime = frame.CreatedTime
	return &f2m.Message, nil
}

type Message2Frame struct {
	Frame frame.SendFrame
}

var _ Converter[*message.Message, *frame.SendFrame] = (*Message2Frame)(nil)

func (m2f *Message2Frame) Convert(mess *message.Message) (*frame.SendFrame, error) {
	m2f.Frame.ReceiverId = mess.SenderId
	if err := proto.Unmarshal(mess.Payload, &m2f.Message); err != nil {
		return nil, err
	}
	m2f.Message.CreatedTime = mess.CreatedTime
	return &m2f.Message, nil
}
