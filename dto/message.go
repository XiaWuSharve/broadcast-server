package dto

import "encoding/json"

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
