package server

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
)

type Server struct {
	upgrader   *websocket.Upgrader
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	rBuf       chan *Message
	wBufSize   int
	Cancel     chan struct{}
	errC       chan error
}

func New(rBufSize int, wBufSize int, RBufSize int, eBufSize int, WBufSize int) *Server {
	server := &Server{
		clients:    make(map[*Client]bool, 0),
		rBuf:       make(chan *Message, RBufSize),
		Cancel:     make(chan struct{}),
		errC:       make(chan error, eBufSize),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		wBufSize:   WBufSize,
	}
	server.SetUpgrader(rBufSize, wBufSize)
	return server
}

func (s *Server) Run(httpServer *http.Server) {
	go s.HandleError()

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

	go func() {
		// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "home.html") })
		http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			err := s.CreateConn(w, r)
			if err != nil {
				fmt.Println(err)
			}
		})
		fmt.Println("server started at ws://localhost:8080/ws")
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			s.errC <- err
		}
	}()
}

func (s *Server) HandleWrite(c *Client) {
Cancel:
	for {
		select {
		case <-s.Cancel:
			break Cancel
		case mes := <-c.WChan:
			err := c.Conn.WriteMessage(websocket.TextMessage, []byte(mes))
			if err != nil {
				s.errC <- fmt.Errorf("write: error during writing: %s", err)
				s.unregister <- c
				break Cancel
			}
		}
	}
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
	if s.register == nil {
		return fmt.Errorf("server: register failed: empty channel")
	}
	s.register <- &Client{conn, make(chan string, s.wBufSize)}
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
}

func (s *Server) HandleRead(c *Client) {
Cancel:
	for {
		select {
		case <-s.Cancel:
			break Cancel
		default:
			_, data, err := c.Conn.ReadMessage()
			if err != nil {
				err = fmt.Errorf("reader: cannot read message: %s", err)
				s.unregister <- c
				s.errC <- err
				break Cancel
			}
			s.rBuf <- &Message{c, string(data)}
		}
	}
}
