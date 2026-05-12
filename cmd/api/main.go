package main

import (
	"context"
	"log"
	"log/slog"

	"go.temporal.io/sdk/client"

	"github.com/reap/cfo-agent/internal/api"
	"github.com/reap/cfo-agent/internal/config"
	"github.com/reap/cfo-agent/internal/store"
)

func main() {
	cfg := config.Load()

	// ── Postgres ──────────────────────────────────────────────────────────
	ctx := context.Background()
	if err := store.Init(ctx, cfg.PostgresDSN); err != nil {
		log.Fatalf("store.Init: %v", err)
	}
	slog.Info("postgres connected")

	// ── Temporal client ───────────────────────────────────────────────────
	tc, err := client.Dial(client.Options{
		HostPort: cfg.TemporalAddress,
	})
	if err != nil {
		log.Fatalf("temporal dial: %v", err)
	}
	defer tc.Close()

	// ── Start API server ──────────────────────────────────────────────────
	server := api.New(tc)
	slog.Info("api server starting", "port", cfg.ServerPort)
	if err := server.Run(cfg.ServerPort); err != nil {
		log.Fatalf("server.Run: %v", err)
	}
}
