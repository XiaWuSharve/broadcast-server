package datas

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/bwmarrin/snowflake"
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

type ReceiveFrameEncoder struct {}

var _ Encoder[*ReceiveFrame] = (*ReceiveFrameEncoder)(nil)

func (p *ReceiveFrameEncoder) ToByte(data *ReceiveFrame) []byte {
	binary.BigEndian.PutUint64(data.FullBuf[0:8], uint64(data.CreatedTime))
	return data.FullBuf
}

type ReceiveFrameParser struct {
	frame ReceiveFrame
}

var _ Decoder[*ReceiveFrame] = (*ReceiveFrameParser)(nil)

func (mp *ReceiveFrameParser) Parse(data []byte) (*ReceiveFrame, error) {
	mp.frame.CreatedTime = int64(binary.BigEndian.Uint64(data[0:8]))
	mp.frame.FullBuf = data
	mp.frame.Payload = data[8:]
	return &mp.frame, nil
}

type MessageEncoder struct{
	Bytes []byte
}

var _ Encoder[*Message] = (*MessageEncoder)(nil)

func (p *MessageEncoder) ToByte(mess *Message) []byte {
	p.Bytes, _ = proto.Marshal(mess)
	return p.Bytes
}

type MessageParser struct {
	message     Message
}

var _ Decoder[*Message] = (*MessageParser)(nil)

func (mp *MessageParser) Parse(data []byte) (*Message, error) {
	if err := proto.Unmarshal(data, &mp.message); err != nil {
		return nil, err
	}
	return &mp.message, nil
}

type ReceiveFrameStreamDecoder struct {
	Rbuf       [12]byte
	Wbuf       [12]byte
	frame      ReceiveFrame
	PayloadLen int
	Err        error
}

var ErrTimeLargeOffset = errors.New("client time too fast or slow")

func ValidateTime(createdTime int64) bool {
	diff := createdTime - time.Now().UnixMilli()
	if diff < 0 {
		diff = -diff
	}
	if diff > config.Cfg.TimeTolerance*1000 { // 5分钟对应的毫秒数
		return false
	}
	return true
}

func (fsd *ReceiveFrameStreamDecoder) Parse(r io.Reader) (*ReceiveFrame, error) {
	// receive frame |CreatedTime 8B|Len 4B -> 1Unit = 1B|
	// send frame |AckType 1B|MessageId 8B|Len 4B -> 1Unit = 1B|
	if _, fsd.Err = io.ReadFull(r, fsd.Rbuf[:]); fsd.Err != nil {
		if errors.Is(fsd.Err, io.ErrClosedPipe) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read header: %w", fsd.Err)
	}
	fsd.frame.CreatedTime = int64(binary.BigEndian.Uint64(fsd.Rbuf[0:8]))
	fsd.PayloadLen = int(binary.BigEndian.Uint32(fsd.Rbuf[8:12]))
	slog.Debug("received", "header", fsd.Rbuf[:], "created time", time.UnixMilli(fsd.frame.CreatedTime).String(), "payload length (Bytes)", fsd.PayloadLen)
	if !ValidateTime(fsd.frame.CreatedTime) {
		io.CopyN(io.Discard, r, int64(fsd.PayloadLen))
		return nil, ErrTimeLargeOffset
	}
	// 预留给生产者字节编码的8字节用于存放created time，避免编码的时候拷贝payload
	fsd.frame.FullBuf = make([]byte, 8+fsd.PayloadLen)
	fsd.frame.Payload = fsd.frame.FullBuf[8:]
	if _, fsd.Err = io.ReadFull(r, fsd.frame.Payload); fsd.Err != nil {
		if errors.Is(fsd.Err, io.ErrClosedPipe) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read payload: %w", fsd.Err)
	}
	return &fsd.frame, nil
}

type ReceiveFrame2Message struct {
	Message Message
}

var _ Converter[*ReceiveFrame, *Message] = (*ReceiveFrame2Message)(nil)

func (f2m *ReceiveFrame2Message) Convert(frame *ReceiveFrame) (*Message, error) {
	if err := proto.Unmarshal(frame.Payload, &f2m.Message); err != nil {
		return nil, err
	}
	f2m.Message.CreatedTime = frame.CreatedTime
	return &f2m.Message, nil
}

type Message2SendFrame struct {
	frame SendFrame
	bytes []byte
	Err error
}

var _ Converter[*Message, *SendFrame] = (*Message2SendFrame)(nil)

func (m2f *Message2SendFrame) Convert(mess *Message) (*SendFrame, error) {
	m2f.frame.AckStatus = mess.GetAck().Status
	m2f.frame.ConnId = mess.ConnId
	m2f.bytes, m2f.Err = proto.Marshal(mess)
	if m2f.Err != nil {
		return nil, m2f.Err
	}
	m2f.frame.Payload = m2f.bytes
	return &m2f.frame, nil
}

var Ids *snowflake.Node

func GenId() int64 {
	return Ids.Generate().Int64()
}