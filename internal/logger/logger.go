// Package logger 基于标准库 log/slog 的统一日志封装。
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// Options 日志初始化选项。
type Options struct {
	Level   string // debug / info / warn / error
	Format  string // json / text
	Service string // 服务名，注入每条日志
}

// New 构造 slog.Logger。
func New(opts Options) *slog.Logger {
	level := parseLevel(opts.Level)

	var handler slog.Handler
	if strings.EqualFold(opts.Format, "text") {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	return slog.New(handler).With("service", opts.Service)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
