package hello

import (
	"fmt"
	"net/http"
)

func startHTTPServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello world!")
	})

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error: ", err)
	}
}
