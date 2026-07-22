/*
Copyright © 2025 Sharve
*/
package main

import (
	"log/slog"

	"github.com/XiaWuSharve/broadcast-server/cmd"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	cmd.Execute()
}
