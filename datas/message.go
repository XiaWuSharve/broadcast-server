package datas

import (
	"google.golang.org/protobuf/proto"
)

type MessageEncoder struct {
	Bytes []byte
}

// GetRequiredBufLen implements [Encodable].
func (p *MessageEncoder) GetRequiredBufLen() int {
	panic("unimplemented")
}

var _ Encodable = (*MessageEncoder)(nil)

func (p *MessageEncoder) ToByte() []byte {
	// p.Bytes, _ = proto.Marshal(mess)
	return p.Bytes
}

type MessageParser struct {
	message Message
}

var _ Decoder[*Message] = (*MessageParser)(nil)

func (mp *MessageParser) Parse(data []byte) (*Message, error) {
	if err := proto.Unmarshal(data, &mp.message); err != nil {
		return nil, err
	}
	return &mp.message, nil
}

// type Message2SendFrame struct {
// 	frame SendFrame
// 	bytes []byte
// 	Err   error
// }

// var _ Converter[*Message, *SendFrame] = (*Message2SendFrame)(nil)

// func (m2f *Message2SendFrame) Convert(mess *Message) (*SendFrame, error) {
// 	m2f.frame.AckStatus = mess.GetAck().Status
// 	m2f.frame.ConnId = mess.ConnId
// 	m2f.bytes, m2f.Err = proto.Marshal(mess)
// 	if m2f.Err != nil {
// 		return nil, m2f.Err
// 	}
// 	m2f.frame.Payload = m2f.bytes
// 	return &m2f.frame, nil
// }

// var Ids *snowflake.Node

// func GenId() int64 {
// 	return Ids.Generate().Int64()
// }
