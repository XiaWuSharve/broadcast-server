package datas

type Send struct {
	ReceiverId string
	AckStatus  AckStatus
	ConnId     int64
	Payload    []byte
}
