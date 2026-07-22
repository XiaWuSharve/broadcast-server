/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/XiaWuSharve/broadcast-server/server"
	"github.com/spf13/cobra"
)

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a broadcast server. ",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		s := server.New(1024, 1024)
		fmt.Println("starting...")
		httpServer := &http.Server{
			Addr: "0.0.0.0:3001",
		}
		s.Start(httpServer)

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		signal.Notify(sig, syscall.SIGTERM)
		<-sig

		fmt.Println("exiting...")
		close(s.Cancel)
		shutDownCtx, timeoutRelease := context.WithTimeout(context.Background(), 10*time.Second)
		if err := httpServer.Shutdown(shutDownCtx); err != nil {
			fmt.Printf("unable to close server: %s", err)
		}
		timeoutRelease()
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
