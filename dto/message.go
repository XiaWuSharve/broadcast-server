package dto

// import (
// 	"encoding/json"
// )

// type MessageType = string

// const (
// 	CONNECT   MessageType = "connect"
// 	CHAT      MessageType = "chat"
// 	CANDIDATE MessageType = "candidate"
// )

// type ConnectMessageRequest struct {
// 	Id          string `json:"id"`
// 	DisplayName string `json:"display_name"`
// }

// type ConnectStatus = string

// const (
// 	SUCCESS       ConnectStatus = "success"
// 	FAIL          ConnectStatus = "fail"
// 	ALREADY_EXIST ConnectStatus = "already_exist"
// )

// type ConnectMessageResponse = ConnectStatus // status

// type CandidateMessage struct {
// 	RemoteId      string `json:"remote_id"`
// 	SdpMid        string `json:"sdp_mid"`
// 	SdpMLineIndex int    `json:"sdp_m_line_index"`
// 	Sdp           string `json:"sdp"`
// }

// type ChatMessageRequest struct {
// 	RemoteId     string        `json:"remote_id"`
// 	DisplayName  string        `json:"display_name"`
// 	MessageChain []MessageUnit `json:"message_chain"`
// }

// type ChatMessageResponse struct {
// 	RemoteId     string        `json:"remote_id"`
// 	DisplayName  string        `json:"display_name"`
// 	MessageChain []MessageUnit `json:"message_chain"`
// }

type Message struct {
	Type        int16
	CreatedTime int64
	Len         int32
	Payload     []byte
}
