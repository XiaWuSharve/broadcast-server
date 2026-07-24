package kcppackage

import (
	"crypto/pbkdf2"
	"crypto/sha1"
	"io"
	"log"
	"time"

	"github.com/xtaci/kcp-go/v5"
)

func Client() {
	key, _ := pbkdf2.Key(sha1.New, "demo pass", []byte("demo salt"), 1024, 32)
	block, _ := kcp.NewAESBlockCrypt(key)
	time.Sleep(time.Second)
	sess, err := kcp.DialWithOptions("127.0.0.1:3001", block, 10, 3)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer sess.Close()

	for {
		data := time.Now().String()
		log.Println("sent:", data)

		_, err := sess.Write([]byte(data))
		if err != nil {
			log.Fatal(err)
			return
		}

		// read back the data
		buf, err := io.ReadAll(sess)
		if err != nil {
			log.Fatal(err)
			return
		}

		log.Println("recv:", string(buf))
	}
}
