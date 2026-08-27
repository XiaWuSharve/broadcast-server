package datas

type Encoder[MessType any] interface {
	ToByte(data MessType) []byte
}

type Converter[S, D any] interface {
	Convert(source S) (D, error)
}

type Decoder[MessType any] interface {
	Parse(data []byte) (MessType, error)
}
