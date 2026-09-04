package datas

type RoutedSend struct {
	Type      MessageType
	AckStatus AckStatus
	Payload   []byte
}

// GetRequiredBufLen implements [Encodable].
func (r *RoutedSend) GetRequiredBufLen() int {
	return 1
}

// ToByte implements [Encodable].
func (r *RoutedSend) ToByte() []byte {
	r.Payload[0] = byte(r.Type<<16) | byte(r.AckStatus)
	return r.Payload
}

var _ Encodable = (*RoutedSend)(nil)
