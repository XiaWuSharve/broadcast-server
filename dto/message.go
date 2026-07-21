package dto

import (
	"encoding/json"
	"time"
)

type MessageType = string

const (
	CONNECT   MessageType = "connect"
	CHAT      MessageType = "chat"
	CANDIDATE MessageType = "candidate"
)

type ConnectMessage struct {
	Id          string `json:"local_id"`
	DisplayName string `json:"display_name"`
}

type CandidateMessage struct {
	RemoteId      string `json:"sessionId"`
	SdpMid        string `json:"sdpMid"`
	SdpMLineIndex int    `json:"sdpMLineIndex"`
	Sdp           string `json:"sdp"`
}

type ChatMessage struct {
	LocalId      string        `json:"local_id"`
	RemoteId     string        `json:"remote_id"`
	MessageChain []MessageUnit `json:"message_chain"`
}

type Message struct {
	CreatedTime time.Time       `json:"created_time"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
}
