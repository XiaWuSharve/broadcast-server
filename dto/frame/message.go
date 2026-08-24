package frame

import "github.com/XiaWuSharve/whisperly/dto/message"

type ReceiveFrame struct {
	Ack         message.Ack
	CreatedTime int64
	Payload     []byte
}

type SendFrame struct {
	Ack        message.AckStatus
	ReceiverId string
	Payload    []byte
}
