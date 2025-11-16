package main

import (
	"fmt"
	"net/http"

	"github.com/XiaWuSharve/broadcast-server/server"
)

func main() {
	s := server.New(1024, 1024, 1024, 1024, 1024)
	defer close(s.Cancel)
	s.Run()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "home.html") })
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		err := s.CreateConn(w, r)
		if err != nil {
			fmt.Println(err)
		}
	})

	http.ListenAndServe(":8080", nil)
	s.Wg.Wait()
}
