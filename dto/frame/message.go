package frame

type Message struct {
	CreatedTime int64
	Len         uint32
	Payload     []byte
}
