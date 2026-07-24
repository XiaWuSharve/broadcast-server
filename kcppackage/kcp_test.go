package kcppackage_test

import (
	"crypto/pbkdf2"
	"crypto/sha1"
	"log"
	"testing"

	"github.com/XiaWuSharve/kcp-webrtc-server/kcppackage"
	"github.com/xtaci/kcp-go/v5"
)

func TestKCPHello(t *testing.T) {
	key, _ := pbkdf2.Key(sha1.New, "demo pass", []byte("demo salt"), 1024, 32)
	block, _ := kcp.NewAESBlockCrypt(key)
	listener, err := kcp.ListenWithOptions("0.0.0.0:3001", block, 10, 3)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer listener.Close()

	// spin-up the client
	go kcppackage.Client()
	s, err := listener.AcceptKCP()
	if err != nil {
		log.Fatal(err)
		return
	}

	kcppackage.HandleEcho(s)

}
