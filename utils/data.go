package utils

import (
	"encoding/binary"

	"github.com/XiaWuSharve/whisperly/dto/frame"
	"github.com/XiaWuSharve/whisperly/dto/message"
	"google.golang.org/protobuf/proto"
)

type Byter[MessType any] interface {
	ToByte(data MessType) []byte
}

type Parser[MessType any] interface {
	Parse(data []byte) (MessType, error)
}

type FrameByter struct {
	Buf [8]byte
}

var _ Byter[*frame.Frame] = (*FrameByter)(nil)

func (p *FrameByter) ToByte(data *frame.Frame) []byte {
	binary.BigEndian.PutUint64(p.Buf[:], uint64(data.CreatedTime))
	return append(p.Buf[:], data.Payload...)
}

type MessageParser struct {
	CreatedTime int64
	Message     message.Message
}

var _ Parser[*message.Message] = (*MessageParser)(nil)

func (mp *MessageParser) Parse(data []byte) (*message.Message, error) {
	mp.CreatedTime = int64(binary.BigEndian.Uint64(data[0:8]))
	if err := proto.Unmarshal(data[8:], &mp.Message); err != nil {
		return nil, err
	}
	mp.Message.CreatedTime = mp.CreatedTime
	return &mp.Message, nil
}
