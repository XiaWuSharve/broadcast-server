package datas

type ReceiveFrame struct {
	CreatedTime int64
	FullBuf     []byte
	Payload     []byte
}

type SendFrame struct {
	AckStatus AckStatus
	ConnId    int64
	Payload   []byte
}
