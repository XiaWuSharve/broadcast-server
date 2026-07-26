package frame

// TODO Len not required
type Message struct {
	CreatedTime int64
	Len         uint32
	Payload     []byte
}
