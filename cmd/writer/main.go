package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ankur-anand/unijord/internal/config"
	"github.com/ankur-anand/unijord/internal/writer"
)

func main() {
	cfg, err := config.LoadWriterConfigFromEnv()
	if err != nil {
		slog.Error("load writer config", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	appender, err := writer.OpenIsleAppender(ctx, cfg)
	if err != nil {
		logger.Error("open writer backend", "error", err)
		os.Exit(1)
	}

	if err := writer.Run(ctx, logger, cfg, appender); err != nil {
		logger.Error("writer exited", "error", err)
		os.Exit(1)
	}
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
