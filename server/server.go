package server

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type Server struct {
	upgrader   *websocket.Upgrader
	conns      map[*websocket.Conn]bool
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	buffer     chan Message
	Cancel     chan struct{}
	errC       chan error
	Wg         *sync.WaitGroup
}

func New(rBufSize int, wBufSize int, mBufSize int, eBufSize int) *Server {
	server := &Server{}
	server.SetUpgrader(rBufSize, wBufSize)
	server.conns = make(map[*websocket.Conn]bool, 0)
	server.buffer = make(chan Message, mBufSize)
	server.Cancel = make(chan struct{})
	server.errC = make(chan error, eBufSize)
	server.Wg = &sync.WaitGroup{}
	server.register = make(chan *websocket.Conn)
	server.unregister = make(chan *websocket.Conn)
	return server
}

func (s *Server) Run() {
	s.Wg.Add(2)
	go s.HandleError()
	go func() {
	Cancel:
		for {
			select {
			case <-s.Cancel:
				break Cancel
			case c := <-s.register:
				s.conns[c] = true
				s.Wg.Add(1)
				go s.HandleRead(c)
			case c := <-s.unregister:
				_, ok := s.conns[c]
				if ok {
					delete(s.conns, c)
				}
			case message := <-s.buffer:
				if message.Err != nil {
					s.errC <- fmt.Errorf("write: receive error message: %s", message.Err)
					continue
				}
				sender := message.Sender
				mes := message.Message
				for c := range s.conns {
					if c == sender {
						continue
					}
					// 如果每个conn writer都有个buffered channel就好了
					s.Wg.Go(func() {
						err := c.WriteMessage(websocket.TextMessage, []byte(mes))
						if err != nil {
							s.errC <- fmt.Errorf("write: error during writing: %s", err)
							s.unregister <- c
							return
						}
					})
				}
			}
		}
		s.Wg.Done()
	}()
}

func (s *Server) SetUpgrader(rBufSize int, wBufSize int) {
	s.upgrader = &websocket.Upgrader{
		ReadBufferSize:  rBufSize,
		WriteBufferSize: wBufSize,
	}
}

func (s *Server) CreateConn(w http.ResponseWriter, r *http.Request) error {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return fmt.Errorf("server: unable to upgrade the HTTP server connection to the WebSocket protocol: %s", err)
	}
	s.register <- conn
	return nil
}

func (s *Server) HandleError() {
Cancel:
	for {
		select {
		case <-s.Cancel:
			break Cancel
		case err := <-s.errC:
			fmt.Printf("log: %s\n", err)
		}
	}

	s.Wg.Done()
}

func (s *Server) HandleRead(conn *websocket.Conn) {
Cancel:
	for {
		select {
		case <-s.Cancel:
			break Cancel
		default:
			_, data, err := conn.ReadMessage()
			if err != nil {
				err = fmt.Errorf("reader: cannot read message: %s", err)
				conn.Close()
				s.unregister <- conn
				s.errC <- err
				break Cancel
			} else if len(data) == 0 {
				continue
			}
			s.buffer <- Message{conn, string(data), err}
		}
	}

	s.Wg.Done()
}
