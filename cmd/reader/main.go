package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ankur-anand/unijord/internal/config"
	"github.com/ankur-anand/unijord/internal/reader"
)

func main() {
	cfg, err := config.LoadReaderConfigFromEnv()
	if err != nil {
		slog.Error("load reader config", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: reader.ParseLogLevel(cfg.LogLevel),
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backend, err := reader.OpenIsleBackend(ctx, cfg)
	if err != nil {
		logger.Error("open reader backend", "error", err)
		os.Exit(1)
	}

	if err := reader.Run(ctx, logger, cfg, backend); err != nil {
		logger.Error("reader exited", "error", err)
		os.Exit(1)
	}
}
