package frame

import "github.com/XiaWuSharve/whisperly/dto/message"

type ReceiveFrame struct {
	CreatedTime int64
	Payload     []byte
}

type SendFrame struct {
	AckStatus  message.AckStatus
	ReceiverId string
	Payload    []byte
}
