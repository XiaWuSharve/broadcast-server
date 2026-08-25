package datas

type ReceiveFrame struct {
	CreatedTime int64
	Payload     []byte
}

type SendFrame struct {
	AckStatus  AckStatus
	ReceiverId int64
	Payload    []byte
}
