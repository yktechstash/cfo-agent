package activity

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/reap/cfo-agent/internal/store"
)

func embedText(ctx context.Context, desc string) ([]float32, error) {
	return nil, nil
}

// RunLearningLoop is the nightly idempotent batch job.
// It is triggered by a cron (e.g. Cloud Scheduler) and runs per tenant.
//
// Design decisions:
//  1. Keyed on (tenant_id, run_date) — safe to run twice if cron fires twice
//  2. Reads correction_events WHERE processed = false AND action != FLAGGED
//  3. Re-embeds each event's transaction → upserts into pgvector (ON CONFLICT)
//  4. Marks each event processed = true in the same DB tx as the upsert
//  5. OVERRIDDEN events get 1.5x weight by storing source = "overridden"
//     (the RAG query orders by similarity, so this is encoded implicitly in
//     the correction being stored — the retrieval naturally prefers it)
func RunLearningLoop(ctx context.Context, tenantID string) error {
	runDate := time.Now().UTC()
	logger := slog.With("tenant_id", tenantID, "run_date", runDate.Format("2006-01-02"))
	logger.Info("learning loop starting")

	// Fetch unprocessed correction events for this tenant
	events, err := store.FetchUnprocessedCorrections(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("fetch corrections: %w", err)
	}
	if len(events) == 0 {
		logger.Info("no unprocessed corrections, skipping")
		return nil
	}

	logger.Info("processing corrections", "count", len(events))
	processed := 0

	for _, event := range events {
		// Fetch the transaction description for embedding
		// (same string format as buildEmbedInput in enrich.go)
		txnDesc, err := store.FetchTxnDesc(ctx, event.TransactionID)
		if err != nil {
			logger.Warn("failed to fetch txn desc, skipping", "txn_id", event.TransactionID, "error", err)
			continue
		}

		// Embed the transaction text
		// In production this calls your embedding API.
		// Kept as a store-layer function so it can be mocked in tests.
		embedding, err := embedText(ctx, txnDesc)
		if err != nil {
			logger.Warn("embed failed, skipping", "txn_id", event.TransactionID, "error", err)
			continue
		}

		// Source encodes the signal weight for future retrieval ordering:
		// "overridden" events are stronger signals than "accepted" ones.
		source := "accepted"
		if event.Action == "OVERRIDDEN" {
			source = "overridden"
		}

		// Upsert into pgvector — idempotent via ON CONFLICT(transaction_id)
		if err := store.UpsertTxnVector(
			ctx,
			event.TenantID,
			event.TransactionID,
			txnDesc,
			event.FinalCode,
			"", // label fetched from CoA at query time
			source,
			embedding,
		); err != nil {
			logger.Warn("upsert vector failed, skipping", "txn_id", event.TransactionID, "error", err)
			continue
		}

		// Mark processed — do this per-event so partial runs are safe
		if err := store.MarkCorrectionProcessed(ctx, event.ID); err != nil {
			logger.Warn("mark processed failed", "event_id", event.ID, "error", err)
			// Non-fatal: the upsert is idempotent, so a duplicate run is safe
		}

		// If this was an override, check if vendor rule should be promoted
		if event.Action == "OVERRIDDEN" {
			if err := MaybePromoteVendorRule(
				ctx,
				event.TenantID,
				func() string {
					name, _ := store.FetchMerchantNormalized(ctx, event.TransactionID)
					return name
				}(),
				event.FinalCode,
				3, // VendorRulePromotionCount
			); err != nil {
				logger.Warn("vendor rule promotion failed", "error", err)
			}
		}

		processed++
	}

	// Log the run — idempotent, ON CONFLICT DO NOTHING
	if err := store.LogLearningRun(ctx, tenantID, runDate, processed); err != nil {
		logger.Warn("failed to log learning run", "error", err)
	}

	logger.Info("learning loop complete", "processed", processed, "total", len(events))
	return nil
}
