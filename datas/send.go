package datas

type SendFrame struct {
	ReceiverId string
	AckStatus  AckStatus
	ConnId     int64
	Payload    []byte
}
