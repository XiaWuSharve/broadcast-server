package datas

type Cache struct {
	ReceiveId string
	Processed bool
	Payload   []byte
}

// GetRequiredBufLen implements [Encodable].
func (c *Cache) GetRequiredBufLen() int {
	panic("unimplemented")
}

// ToByte implements [Encodable].
func (c *Cache) ToByte() []byte {
	panic("unimplemented")
}

var _ Encodable = (*Cache)(nil)
