package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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

func UpsertTransaction(ctx context.Context, txn domain.Transaction) error {
	transactionDate, err := time.Parse("2006-01-02", txn.TransactionDate)
	if err != nil {
		return fmt.Errorf("invalid transaction_date %q: %w", txn.TransactionDate, err)
	}

	var postedAt any
	if !txn.PostedAt.IsZero() {
		postedAt = txn.PostedAt
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO transactions (
			transaction_id, tenant_id, source, merchant_name, merchant_normalized_name,
			mcc, mcc_description, amount, currency, billing_amount, billing_currency,
			fx_rate, transaction_date, posted_at, card_last4, description, receipt_id, receipt_status
		) VALUES (
			$1,$2,$3,$4,$5,
			$6,$7,$8,$9,$10,$11,
			$12,$13,$14,$15,$16,$17,$18
		)
		ON CONFLICT (transaction_id) DO UPDATE SET
			tenant_id = EXCLUDED.tenant_id,
			source = EXCLUDED.source,
			merchant_name = EXCLUDED.merchant_name,
			merchant_normalized_name = EXCLUDED.merchant_normalized_name,
			mcc = EXCLUDED.mcc,
			mcc_description = EXCLUDED.mcc_description,
			amount = EXCLUDED.amount,
			currency = EXCLUDED.currency,
			billing_amount = EXCLUDED.billing_amount,
			billing_currency = EXCLUDED.billing_currency,
			fx_rate = EXCLUDED.fx_rate,
			transaction_date = EXCLUDED.transaction_date,
			posted_at = EXCLUDED.posted_at,
			card_last4 = EXCLUDED.card_last4,
			description = EXCLUDED.description,
			receipt_id = EXCLUDED.receipt_id,
			receipt_status = EXCLUDED.receipt_status
	`,
		txn.TransactionID, txn.TenantID, txn.Source, txn.MerchantName, txn.MerchantNormalizedName,
		txn.MCC, txn.MCCDescription, txn.Amount, txn.Currency, txn.BillingAmount, txn.BillingCurrency,
		txn.FXRate, transactionDate, postedAt, txn.CardLast4, txn.Description, txn.ReceiptID, txn.ReceiptStatus,
	)
	return err
}

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
		return domain.ConfidenceThreshold{AutoApproveMin: 0.90, ReviewMin: 0.65}, nil
	}
	return t, err
}

func UpsertThresholds(ctx context.Context, tenantID string, autoApproveMin, reviewMin float64) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO confidence_thresholds (tenant_id, auto_approve_min, review_min, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (tenant_id) DO UPDATE
		SET auto_approve_min = EXCLUDED.auto_approve_min,
		    review_min = EXCLUDED.review_min,
		    updated_at = EXCLUDED.updated_at
	`, tenantID, autoApproveMin, reviewMin)
	return err
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

func UpdateReviewQueueStatus(ctx context.Context, txnID string, status string) error {
	_, err := pool.Exec(ctx, `
		UPDATE review_queue
		SET status = $2
		WHERE transaction_id = $1
	`, txnID, status)
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

func FetchWorklist(ctx context.Context, tenantID string, limit int) ([]domain.TxnListItem, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := pool.Query(ctx, `
		SELECT
			t.transaction_id,
			t.tenant_id,
			t.merchant_name,
			t.amount,
			t.currency,
			t.transaction_date,
			COALESCE(tg.status, rq.status) AS status,
			rq.status AS route,
			'' AS tag,
			COALESCE(rq.suggested_code, ''),
			COALESCE(rq.suggested_label, ''),
			COALESCE(rq.confidence, 0),
			COALESCE(rq.rationale, ''),
			COALESCE(rq.queued_at, tg.updated_at) AS updated_at
		FROM transactions t
		LEFT JOIN tagging tg ON tg.transaction_id = t.transaction_id
		JOIN review_queue rq ON rq.transaction_id = t.transaction_id
		WHERE t.tenant_id = $1
		  AND rq.status IN ('IN_REVIEW', 'ESCALATED')

		UNION ALL

		SELECT
			t.transaction_id,
			t.tenant_id,
			t.merchant_name,
			t.amount,
			t.currency,
			t.transaction_date,
			tg.status,
			CASE WHEN rq.status = 'RESOLVED' THEN 'MANUAL_REVIEW' ELSE 'AUTO_TAGGED' END AS route,
			CASE WHEN rq.status = 'RESOLVED' THEN 'Moved From Review' ELSE 'Auto Tagged' END AS tag,
			COALESCE(rq.suggested_code, ''),
			COALESCE(rq.suggested_label, ''),
			COALESCE(rq.confidence, 0),
			COALESCE(rq.rationale, ''),
			COALESCE(tg.updated_at, rq.queued_at) AS updated_at
		FROM transactions t
		JOIN tagging tg ON tg.transaction_id = t.transaction_id
		LEFT JOIN review_queue rq ON rq.transaction_id = t.transaction_id
		WHERE t.tenant_id = $1
		  AND tg.status = 'SUCCESS'
		  AND (rq.transaction_id IS NULL OR rq.status = 'RESOLVED')

		ORDER BY updated_at DESC
		LIMIT $2
	`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.TxnListItem
	for rows.Next() {
		var item domain.TxnListItem
		var statusStr string
		var transactionDate time.Time
		if err := rows.Scan(
			&item.TransactionID, &item.TenantID, &item.MerchantName,
			&item.Amount, &item.Currency, &transactionDate,
			&statusStr, &item.Route, &item.Tag, &item.SuggestedCode,
			&item.SuggestedLabel, &item.Confidence, &item.Rationale,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.TransactionDate = transactionDate.Format("2006-01-02")
		item.Status = domain.TxnStatus(statusStr)
		out = append(out, item)
	}
	return out, nil
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
	embedding, err := EmbedText(ctx, embedInput)
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
const embeddingDim = 1536

var embeddingHTTPClient = &http.Client{Timeout: 30 * time.Second}

type teiEmbedRequest struct {
	Inputs string `json:"inputs"`
}

type teiEmbedResponse struct {
	Vector  []float64
	Vectors [][]float64
}

func (r *teiEmbedResponse) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return errors.New("empty response")
	}
	if b[0] == '[' {
		var v []float64
		if err := json.Unmarshal(b, &v); err == nil {
			r.Vector = v
			return nil
		}
		var vv [][]float64
		if err := json.Unmarshal(b, &vv); err == nil {
			r.Vectors = vv
			return nil
		}
	}
	return fmt.Errorf("unexpected TEI response: %s", string(b))
}

func EmbedText(ctx context.Context, text string) ([]float32, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, errors.New("embed: empty text")
	}

	endpoint := strings.TrimSpace(os.Getenv("EMBEDDINGS_URL"))
	if endpoint == "" {
		endpoint = "http://localhost:8082/embed"
	}

	payload, err := json.Marshal(teiEmbedRequest{Inputs: text})
	if err != nil {
		return nil, fmt.Errorf("embed: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("embed: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := embeddingHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, fmt.Errorf("embed: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("embed: TEI %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var decoded teiEmbedResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("embed: decode response: %w", err)
	}

	vec64 := decoded.Vector
	if vec64 == nil && len(decoded.Vectors) > 0 {
		vec64 = decoded.Vectors[0]
	}
	if len(vec64) == 0 {
		return nil, errors.New("embed: empty embedding")
	}

	out := make([]float32, len(vec64))
	for i, f := range vec64 {
		out[i] = float32(f)
	}
	return fixEmbeddingDim(out, embeddingDim), nil
}

func fixEmbeddingDim(v []float32, dim int) []float32 {
	if len(v) == dim {
		return v
	}
	if len(v) > dim {
		return v[:dim]
	}
	padded := make([]float32, dim)
	copy(padded, v)
	return padded
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
