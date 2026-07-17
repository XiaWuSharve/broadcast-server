/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha1"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/XiaWuSharve/broadcast-server/server"
	"github.com/spf13/cobra"

	"github.com/xtaci/kcp-go/v5"
)

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a broadcast server. ",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		use_ws := cmd.Flag("use_ws").Value.String()

		if use_ws == "true" {
			fmt.Println("starting websocket server...")
			s := server.New(1024, 1024, 1024, 1024, 1024)
			httpServer := &http.Server{
				Addr: ":8080",
			}
			s.Run(httpServer)
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt)
			signal.Notify(sig, syscall.SIGTERM)
			<-sig

			fmt.Println("exiting...")
			close(s.Cancel)
			shutDownCtx, timeoutRelease := context.WithTimeout(context.Background(), 10*time.Second)
			if err := httpServer.Shutdown(shutDownCtx); err != nil {
				log.Fatalf("unable to close server: %s", err)
			}
			timeoutRelease()
		} else {
			fmt.Println("starting kcp server...")
			key, err := pbkdf2.Key(sha1.New, "demo pass", []byte("demo salt"), 1024, 32)
			if err != nil {
				log.Fatal("failed to generate key:", err)
				return
			}
			block, _ := kcp.NewAESBlockCrypt(key)

			listener, err := kcp.ListenWithOptions("127.0.0.1:8080", block, 10, 3)
			if err != nil {
				log.Fatal(err)
				return
			}
			defer listener.Close()
			for {
				s, err := listener.AcceptKCP()
				if err != nil {
					log.Fatal(err)
					return
				}
				go server.HandleEcho(s)
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(startCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// startCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// startCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
