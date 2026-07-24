package kcppackage

import (
	"log"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// handleEcho send back everything it received
func HandleEcho(conn *kcp.UDPSession) {
	buf := make([]byte, 4096)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			log.Println(err)
			return
		}

		_, err = conn.Write([]byte(time.Now().String()))
		if err != nil {
			log.Println(err)
			return
		}
	}
}
