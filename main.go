/*
Copyright © 2025 Sharve
*/
package main

import (
	"log/slog"

	"github.com/XiaWuSharve/kcp-webrtc-server/cmd"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	cmd.Execute()
}
