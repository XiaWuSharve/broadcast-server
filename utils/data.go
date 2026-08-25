package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
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

type FrameStreamDecoder struct {
	Rbuf       [12]byte
	Wbuf       [12]byte
	frame      frame.ReceiveFrame
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

func (fsd *FrameStreamDecoder) Parse(r io.Reader) (*frame.ReceiveFrame, error) {
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

	fsd.frame.Payload = make([]byte, fsd.PayloadLen)
	if _, fsd.Err = io.ReadFull(r, fsd.frame.Payload); fsd.Err != nil {
		if errors.Is(fsd.Err, io.ErrClosedPipe) {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("failed to read payload: %w", fsd.Err)
	}
	return &fsd.frame, nil
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
	m2f.Frame.AckStatus = mess.GetAck().Status
	m2f.Frame.ReceiverId = mess.ReceiverId
	bytes, err := proto.Marshal(mess)
	if err != nil {
		return nil, err
	}
	m2f.Frame.Payload = bytes
	return &m2f.Frame, nil
}
