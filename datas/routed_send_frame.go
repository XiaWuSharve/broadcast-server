package datas

type RoutedSendFrame struct {
	Ack     AckStatus
	Payload []byte
}
