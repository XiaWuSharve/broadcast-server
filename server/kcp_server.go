package server

import (
	"log"

	"github.com/xtaci/kcp-go/v5"
)

type KCPServer struct {
	Addr         string
	DataShards   int
	Block        kcp.BlockCrypt
	ParityShards int
	wrChan       chan []byte
	rdChan       chan []byte
	conn         *kcp.UDPSession
	clients      map[*Client]bool
}

var _ ServerInterface = (*KCPServer)(nil)

func NewKCPServer() *KCPServer {
	return &KCPServer{}
}

func (srv *KCPServer) Start() error {
	listener, err := kcp.ListenWithOptions(srv.Addr, srv.Block, srv.DataShards, srv.ParityShards)
	if err != nil {
		log.Println(err)
		return err
	}
	defer listener.Close()
	for {
		s, err := listener.AcceptKCP()
		if err != nil {
			log.Println(err)
			return err
		}
		srv.conn = s
		go srv.HandleRead()
		go srv.HandleWrite()
	}

}

func (s *KCPServer) Listen() {

}

func (srv *KCPServer) HandleRead(c *Client) {
	buf := make([]byte, 4096)
	for {
		n, err := srv.conn.Read(buf)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func (srv *KCPServer) HandleWrite(c *Client) {
	for {
		select {
		case data := <-c.WChan:
			c.Conn.S
		}
	}
	_, err = srv.conn.Write(buf[:n])
	if err != nil {
		log.Println(err)
		return
	}
}
