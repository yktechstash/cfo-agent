package main

import (
	"context"
	"log"
	"log/slog"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/reap/cfo-agent/internal/activity"
	"github.com/reap/cfo-agent/internal/config"
	"github.com/reap/cfo-agent/internal/llm"
	"github.com/reap/cfo-agent/internal/store"
	wf "github.com/reap/cfo-agent/internal/workflow"
)

func main() {
	cfg := config.Load()

	// ── Postgres ──────────────────────────────────────────────────────────
	ctx := context.Background()
	if err := store.Init(ctx, cfg.PostgresDSN); err != nil {
		log.Fatalf("store.Init: %v", err)
	}
	slog.Info("postgres connected")

	// ── LLM client ────────────────────────────────────────────────────────
	llm.Init(cfg)
	slog.Info("llm client initialised")

	// ── Temporal client ───────────────────────────────────────────────────
	tc, err := client.Dial(client.Options{
		HostPort: cfg.TemporalAddress,
	})
	if err != nil {
		log.Fatalf("temporal dial: %v", err)
	}
	defer tc.Close()

	// ── Worker registration ────────────────────────────────────────────────
	// One worker handles both the workflow orchestration and all activities.
	// In production you'd separate the LLM-heavy activities onto their own
	// task queue with dedicated concurrency limits and cost tracking.
	w := worker.New(tc, wf.TaskQueueTagging, worker.Options{
		MaxConcurrentActivityExecutionSize:      20,
		MaxConcurrentWorkflowTaskExecutionSize:  50,
	})

	// Register workflow
	w.RegisterWorkflow(wf.TaggingWorkflow)

	// Register activities — Temporal discovers them by function reference
	w.RegisterActivity(activity.SetTxnStatus)
	w.RegisterActivity(activity.FetchThresholds)
	w.RegisterActivity(activity.EnrichTransaction)
	w.RegisterActivity(activity.TagTransaction)
	w.RegisterActivity(activity.WriteReviewQueueItem)
	w.RegisterActivity(activity.WriteEscalationQueueItem)
	w.RegisterActivity(activity.UpdateReviewQueueStatus)
	w.RegisterActivity(activity.WriteCorrectionEvent)
	w.RegisterActivity(activity.MaybePromoteVendorRule)

	slog.Info("worker starting", "task_queue", wf.TaskQueueTagging)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker run: %v", err)
	}
}
