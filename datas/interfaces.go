package datas

type Encoder interface {
	ToByte() []byte
	GetRequiredBufLen() int
}

type Converter[S, D any] interface {
	Convert(source S) (D, error)
}

type Decoder[MessType any] interface {
	Parse(data []byte) (MessType, error)
}
