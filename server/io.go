package server

type IO interface {
	Read() ([]byte, error)
	Write([]byte) error
}
