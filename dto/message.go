package dto

import "encoding/json"

// TODO 添加时延
type Message struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

const (
	CONNECT   = "connect"
	CALL      = "call"
	ANSWER    = "answer"
	CANDIDATE = "candidate"
)
