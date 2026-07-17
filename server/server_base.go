package server

import (
	"log"
)

type ServerInterface interface {
	Start() error
	Listen()
	HandleWrite(c *Client)
	HandleRead(c *Client)
}

type ServerBase struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	rBuf       chan *Message
	wBufSize   int
	Cancel     chan struct{}
}

var _ ServerInterface = (*ServerBase)(nil)

func (s *ServerBase) Start() error {
	go func() {
	Cancel:
		for {
			select {
			case <-s.Cancel:
				for c := range s.clients {
					c.Conn.Close()
				}
				break Cancel
			case c := <-s.register:
				s.clients[c] = true
				go s.HandleWrite(c)
				go s.HandleRead(c)
			case c := <-s.unregister:
				_, ok := s.clients[c]
				if ok {
					c.Conn.Close()
					close(c.WChan)
					delete(s.clients, c)
				}
			case m := <-s.rBuf:
				sender := m.Sender
				mes := m.Message
				for c := range s.clients {
					if c == sender {
						continue
					}
					c.WChan <- mes
				}
			}
		}
	}()

	go s.Listen()
	return nil
}

func (s *ServerBase) Listen() {
	log.Fatal("not implemented")
}

func (s *ServerBase) HandleWrite(c *Client) {
	log.Fatal("not implemented")
}

func (s *ServerBase) HandleRead(c *Client) {
	log.Fatal("not implemented")
}
