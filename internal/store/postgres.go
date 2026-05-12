package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"

	"github.com/reap/cfo-agent/internal/domain"
)

var pool *pgxpool.Pool

// Init opens the Postgres connection pool.
// pgvector extension must be enabled: CREATE EXTENSION IF NOT EXISTS vector;
func Init(ctx context.Context, dsn string) error {
	var err error
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	return pool.Ping(ctx)
}

// ── Transaction state ──────────────────────────────────────────────────────

// SetTxnStatus is an append-only upsert — we write to tagging table
// and append a row to status_history for the audit trail.
func SetTxnStatus(ctx context.Context, txnID string, status domain.TxnStatus) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO tagging (transaction_id, status, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (transaction_id) DO UPDATE
		SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
	`, txnID, string(status))
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO status_history (transaction_id, status, recorded_at)
		VALUES ($1, $2, NOW())
	`, txnID, string(status))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ── CoA ───────────────────────────────────────────────────────────────────

func FetchCoAEntries(ctx context.Context, tenantID string) ([]domain.CoAEntry, error) {
	rows, err := pool.Query(ctx, `
		SELECT code, label FROM coa_schema
		WHERE tenant_id = $1
		ORDER BY code
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []domain.CoAEntry
	for rows.Next() {
		var e domain.CoAEntry
		if err := rows.Scan(&e.Code, &e.Label); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no CoA entries for tenant %s", tenantID)
	}
	return entries, nil
}

// ── Confidence thresholds ─────────────────────────────────────────────────

func FetchThresholds(ctx context.Context, tenantID string) (domain.ConfidenceThreshold, error) {
	var t domain.ConfidenceThreshold
	err := pool.QueryRow(ctx, `
		SELECT tenant_id, auto_approve_min, review_min
		FROM confidence_thresholds
		WHERE tenant_id = $1 OR tenant_id = ''
		ORDER BY tenant_id DESC
		LIMIT 1
	`, tenantID).Scan(&t.TenantID, &t.AutoApproveMin, &t.ReviewMin)
	if err == pgx.ErrNoRows {
		// Absolute fallback
		return domain.ConfidenceThreshold{AutoApproveMin: 0.90, ReviewMin: 0.65}, nil
	}
	return t, err
}

// ── Vendor rules ──────────────────────────────────────────────────────────

func FetchVendorRule(ctx context.Context, tenantID, merchantNormalized string) (*domain.VendorRule, error) {
	var rule domain.VendorRule
	err := pool.QueryRow(ctx, `
		SELECT tenant_id, merchant_normalized, coa_code, override_count, promoted_at
		FROM vendor_rules
		WHERE tenant_id = $1 AND merchant_normalized = $2
		LIMIT 1
	`, tenantID, merchantNormalized).Scan(
		&rule.TenantID, &rule.MerchantNormalized, &rule.CoACode,
		&rule.OverrideCount, &rule.PromotedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rule, nil
}

func CountOverrides(ctx context.Context, tenantID, merchantNormalized, coaCode string) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM correction_events
		WHERE tenant_id = $1
		  AND merchant_normalized = $2
		  AND final_code = $3
		  AND action = 'OVERRIDDEN'
	`, tenantID, merchantNormalized, coaCode).Scan(&count)
	return count, err
}

func UpsertVendorRule(ctx context.Context, rule domain.VendorRule) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO vendor_rules (tenant_id, merchant_normalized, coa_code, override_count, promoted_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, merchant_normalized) DO UPDATE
		SET coa_code = EXCLUDED.coa_code,
		    override_count = EXCLUDED.override_count,
		    promoted_at = EXCLUDED.promoted_at
	`, rule.TenantID, rule.MerchantNormalized, rule.CoACode, rule.OverrideCount, rule.PromotedAt)
	return err
}

// ── Review queue ──────────────────────────────────────────────────────────

func WriteReviewQueueItem(ctx context.Context, item domain.ReviewQueueItem) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO review_queue (
			transaction_id, tenant_id, merchant_name, amount, currency,
			transaction_date, suggested_code, suggested_label,
			confidence, rationale, status, queued_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (transaction_id) DO UPDATE
		SET status = EXCLUDED.status, queued_at = EXCLUDED.queued_at
	`,
		item.TransactionID, item.TenantID, item.MerchantName,
		item.Amount, item.Currency, item.Date,
		item.SuggestedCode, item.SuggestedLabel,
		item.Confidence, item.Rationale,
		string(item.Status), item.QueuedAt,
	)
	return err
}

func FetchReviewQueue(ctx context.Context, tenantID string) ([]domain.ReviewQueueItem, error) {
	rows, err := pool.Query(ctx, `
		SELECT transaction_id, tenant_id, merchant_name, amount, currency,
		       transaction_date, suggested_code, suggested_label,
		       confidence, rationale, status, queued_at
		FROM review_queue
		WHERE tenant_id = $1 AND status = 'IN_REVIEW'
		ORDER BY queued_at ASC
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []domain.ReviewQueueItem
	for rows.Next() {
		var item domain.ReviewQueueItem
		var statusStr string
		if err := rows.Scan(
			&item.TransactionID, &item.TenantID, &item.MerchantName,
			&item.Amount, &item.Currency, &item.Date,
			&item.SuggestedCode, &item.SuggestedLabel,
			&item.Confidence, &item.Rationale, &statusStr, &item.QueuedAt,
		); err != nil {
			return nil, err
		}
		item.Status = domain.TxnStatus(statusStr)
		items = append(items, item)
	}
	return items, nil
}

// ── Correction events ─────────────────────────────────────────────────────

func WriteCorrectionEvent(ctx context.Context, event domain.CorrectionEvent) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO correction_events (
			id, transaction_id, tenant_id, action,
			suggested_code, final_code, accountant_id,
			created_at, processed
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`,
		event.ID, event.TransactionID, event.TenantID,
		string(event.Action), event.SuggestedCode, event.FinalCode,
		event.AccountantID, event.CreatedAt, event.Processed,
	)
	return err
}

// FetchUnprocessedCorrections fetches correction events for the nightly
// learning loop. Only events not yet processed and not FLAGGED are returned.
func FetchUnprocessedCorrections(ctx context.Context, tenantID string) ([]domain.CorrectionEvent, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, transaction_id, tenant_id, action,
		       suggested_code, final_code, accountant_id, created_at
		FROM correction_events
		WHERE tenant_id = $1
		  AND processed = false
		  AND action != 'FLAGGED'
		ORDER BY created_at ASC
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.CorrectionEvent
	for rows.Next() {
		var e domain.CorrectionEvent
		var actionStr string
		if err := rows.Scan(
			&e.ID, &e.TransactionID, &e.TenantID, &actionStr,
			&e.SuggestedCode, &e.FinalCode, &e.AccountantID, &e.CreatedAt,
		); err != nil {
			return nil, err
		}
		e.Action = domain.CorrectionAction(actionStr)
		events = append(events, e)
	}
	return events, nil
}

func MarkCorrectionProcessed(ctx context.Context, id string) error {
	_, err := pool.Exec(ctx,
		`UPDATE correction_events SET processed = true WHERE id = $1`, id)
	return err
}

// ── RAG / pgvector ────────────────────────────────────────────────────────

// FetchRAGNeighbours runs a kNN cosine similarity search in pgvector,
// filtered strictly to the tenant's namespace.
//
// The embed function is injected to keep the store layer testable
// without hitting the embedding API.
func FetchRAGNeighbours(
	ctx context.Context,
	tenantID string,
	embedInput string,
	k int,
) ([]domain.RAGNeighbour, error) {
	embedding, err := embedText(ctx, embedInput)
	if err != nil {
		return nil, fmt.Errorf("embed failed: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT txn_desc, coa_code, coa_label, source,
		       1 - (embedding <=> $1) AS similarity
		FROM txn_vectors
		WHERE tenant_id = $2
		ORDER BY embedding <=> $1
		LIMIT $3
	`, pgvector.NewVector(embedding), tenantID, k)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var neighbours []domain.RAGNeighbour
	for rows.Next() {
		var n domain.RAGNeighbour
		if err := rows.Scan(&n.TransactionDesc, &n.CoACode, &n.CoALabel, &n.Source, &n.Similarity); err != nil {
			return nil, err
		}
		neighbours = append(neighbours, n)
	}
	return neighbours, nil
}

// UpsertTxnVector inserts or updates a vector in the tenant's namespace.
// Called by the nightly learning loop with confirmed correction events.
// ON CONFLICT on transaction_id ensures idempotency — safe to run twice.
func UpsertTxnVector(
	ctx context.Context,
	tenantID, txnID, txnDesc, coaCode, coaLabel, source string,
	embedding []float32,
) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO txn_vectors (
			tenant_id, transaction_id, txn_desc,
			coa_code, coa_label, source, embedding, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
		ON CONFLICT (transaction_id) DO UPDATE
		SET coa_code = EXCLUDED.coa_code,
		    coa_label = EXCLUDED.coa_label,
		    source = EXCLUDED.source,
		    embedding = EXCLUDED.embedding,
		    updated_at = EXCLUDED.updated_at
	`, tenantID, txnID, txnDesc, coaCode, coaLabel, source, pgvector.NewVector(embedding))
	return err
}

// ── Receipt ───────────────────────────────────────────────────────────────

type Receipt struct {
	ReceiptID       string
	VendorNormalised string
	Currency        string
	Total           float64
	TaxType         string
	AmountVerified  bool
	LineItems       []LineItem
}

type LineItem struct {
	Description string
	Amount      float64
}

func FetchReceipt(ctx context.Context, receiptID string) (*Receipt, error) {
	// In production this fetches from the receipt_ocr table.
	// Simplified scan here — extend with line_items join as needed.
	var r Receipt
	err := pool.QueryRow(ctx, `
		SELECT receipt_id, vendor_normalised, currency, total, tax_type, amount_verified
		FROM receipt_ocr
		WHERE receipt_id = $1
	`, receiptID).Scan(
		&r.ReceiptID, &r.VendorNormalised, &r.Currency,
		&r.Total, &r.TaxType, &r.AmountVerified,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return &r, err
}

// ── Embedding (stub — replace with real embedding API call) ───────────────

// embedText calls your embedding provider (e.g. OpenAI text-embedding-3-small
// or a self-hosted model). Kept separate so it can be mocked in tests.
func embedText(ctx context.Context, text string) ([]float32, error) {
	// TODO: replace with real embedding API call
	// Example using OpenAI:
	//   resp, err := openaiClient.CreateEmbedding(ctx, text)
	//   return resp.Embedding, err
	//
	// For now, return a zero vector so tests compile.
	_ = text
	return make([]float32, 1536), nil
}

// ── Nightly learning loop helpers ─────────────────────────────────────────

// FetchAllTenantIDs returns all active tenant IDs for the nightly loop cron.
func FetchAllTenantIDs(ctx context.Context) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT DISTINCT tenant_id FROM coa_schema`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// FetchTxnDesc fetches the embed input string for a transaction.
// Used by the learning loop when building the vector to upsert.
func FetchTxnDesc(ctx context.Context, txnID string) (string, error) {
	var desc string
	err := pool.QueryRow(ctx, `
		SELECT merchant_normalized_name || ' ' || mcc_description || ' ' || description
		FROM transactions
		WHERE transaction_id = $1
	`, txnID).Scan(&desc)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("transaction %s not found", txnID)
	}
	return desc, err
}

// FetchMerchantNormalized fetches the normalized merchant name for a transaction.
func FetchMerchantNormalized(ctx context.Context, txnID string) (string, error) {
	var name string
	err := pool.QueryRow(ctx, `
		SELECT merchant_normalized_name FROM transactions WHERE transaction_id = $1
	`, txnID).Scan(&name)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return name, err
}

// LogLearningRun records that the nightly loop ran for a tenant,
// making the job idempotent (ON CONFLICT DO NOTHING).
func LogLearningRun(ctx context.Context, tenantID string, runDate time.Time, eventsProcessed int) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO learning_run_log (tenant_id, run_date, events_processed, ran_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (tenant_id, run_date) DO NOTHING
	`, tenantID, runDate.Format("2006-01-02"), eventsProcessed)
	return err
}
