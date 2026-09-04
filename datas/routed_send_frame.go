package datas

type RoutedSend struct {
	Type      MessageType
	AckStatus AckStatus
	Payload   []byte
}

// GetRequiredBufLen implements [Encoder].
func (r *RoutedSend) GetRequiredBufLen() int {
	return 1
}

// ToByte implements [Encoder].
func (r *RoutedSend) ToByte() []byte {
	r.Payload[0] = byte(r.Type<<16) | byte(r.AckStatus)
	return r.Payload
}

var _ Encoder = (*RoutedSend)(nil)
