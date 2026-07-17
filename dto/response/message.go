package response

type Message struct {
	Type string              `json:"type"`
	Data map[string]struct{} `json:"data"`
}

type ConnectMessage = string

type CallMessage struct {
	Sdp      string `json:"sdp"`
	RemoteId string `json:"remoteId"`
}

type AnswerMessage = string

type CandidateMessage struct {
	SdpMid        string `json:"sdpMid"`
	SdpMLineIndex int    `json:"sdpMLineIndex"`
	Sdp           string `json:"sdp"`
}
