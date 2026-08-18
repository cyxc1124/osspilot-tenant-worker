package logx

import (
	"log/slog"
	"os"
	"strings"
)

func Setup(service string) {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	format := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_FORMAT")))
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		format = "text"
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(h).With("service", service))
	slog.Info("log ready", "level", level.String(), "format", format)
}
