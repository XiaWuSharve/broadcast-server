package dto

import (
	"encoding/json"
)

type MessageType = string

const (
	CONNECT   MessageType = "connect"
	CHAT      MessageType = "chat"
	CANDIDATE MessageType = "candidate"
)

type ConnectMessageRequest struct {
	Id          string `json:"id"`
	DisplayName string `json:"display_name"`
}

type ConnectStatus = string

const (
	SUCCESS       ConnectStatus = "success"
	FAIL          ConnectStatus = "fail"
	ALREADY_EXIST ConnectStatus = "already_exist"
)

type ConnectMessageResponse = ConnectStatus // status

type CandidateMessage struct {
	RemoteId      string `json:"sessionId"`
	SdpMid        string `json:"sdpMid"`
	SdpMLineIndex int    `json:"sdpMLineIndex"`
	Sdp           string `json:"sdp"`
}

type ChatMessageRequest struct {
	RemoteId     string        `json:"remote_id"`
	DisplayName  string        `json:"display_name"`
	MessageChain []MessageUnit `json:"message_chain"`
}

type ChatMessageResponse struct {
	RemoteId     string        `json:"remote_id"`
	DisplayName  string        `json:"display_name"`
	MessageChain []MessageUnit `json:"message_chain"`
}

type Message struct {
	CreatedTime int64           `json:"created_time"`
	Type        string          `json:"type"`
	Data        json.RawMessage `json:"data"`
}
