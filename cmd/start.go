/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"errors"
	"log/slog"
	"net"
	"net/http"

	"github.com/XiaWuSharve/kcp-webrtc-server/kcpserver"
	"github.com/XiaWuSharve/kcp-webrtc-server/server"
	"github.com/spf13/cobra"
	"github.com/xtaci/kcp-go/v5"
)

var protocol string

type Server interface {
	Start(net.Listener) error
}

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a broadcast server. ",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		var s Server
		var listener net.Listener
		var err error
		slog.Info("starting...", "protocol", protocol)
		switch protocol {
		case "kcp":
			listener, err = kcp.Listen("0.0.0.0:3001")
			if err != nil {
				slog.Error("cannot create listener", "err", err)
				// 处理错误...
			}
			s = kcpserver.New()
		case "websocket":
			listener, err = net.Listen("tcp", "0.0.0.0:3001")
			if err != nil {
				slog.Error("cannot create listener", "err", err)
				// 处理错误...
			}
			s = server.New(1024, 1024)
		default:
			slog.Error("protocol param cannot be", "value", protocol)
			return
		}

		if err := s.Start(listener); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("cannot serve", "err", err)
		}
		defer func() {
			if err := listener.Close(); err != nil {
				slog.Error("unable to close server", "err", err)
			}
		}()

		// sig := make(chan os.Signal, 1)
		// signal.Notify(sig, os.Interrupt)
		// signal.Notify(sig, syscall.SIGTERM)
		// <-sig

		slog.Info("exiting...")
		// close(s.Cancel)
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

	// 添加 --name flag
	// &name: 将 flag 的值绑定到 name 变量
	// "name": flag 的长名称 --name
	// "n": flag 的短名称 -n
	// "": flag 的默认值
	// "Name to greet": flag 的帮助信息
	startCmd.Flags().StringVarP(&protocol, "protocol", "p", "kcp", "protocol for the server (support: kcp, websocket)")
}
