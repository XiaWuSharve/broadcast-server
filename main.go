/*
Copyright © 2025 Sharve
*/
package main

import (
	"log/slog"

	"github.com/XiaWuSharve/whisperly/cmd"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	cmd.Execute()
}
