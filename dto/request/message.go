package request

import "github.com/XiaWuSharve/broadcast-server/server"

type ClientMessage struct {
	Sender  *server.Client
	Message string
}

type Message struct {
	Type string              `json:"type"`
	Data map[string]struct{} `json:"data"`
}

type ConnectMessage = string

type CallMessage struct {
	Sdp      string `json:"sdp"`
	RemoteId string `json:"remoteId"`
	LocalId  string `json:"localId"`
}

type AnswerMessage struct {
	Sdp      string `json:"sdp"`
	RemoteId string `json:"sessionId"`
}

type CandidateMessage struct {
	RemoteId      string `json:"sessionId"`
	SdpMid        string `json:"sdpMid"`
	SdpMLineIndex int    `json:"sdpMLineIndex"`
	Sdp           string `json:"sdp"`
}
