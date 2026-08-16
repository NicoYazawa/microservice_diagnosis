// Package logger is a thin wrapper around the standard library log/slog.
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// Options holds logger initialization options.
type Options struct {
	Level   string // debug / info / warn / error
	Format  string // json / text
	Service string // service name injected into every log record
}

// New builds a slog.Logger.
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
