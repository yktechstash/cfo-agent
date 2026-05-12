package activity

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/reap/cfo-agent/internal/domain"
	"github.com/reap/cfo-agent/internal/store"
)

// SetTxnStatus persists a state transition for a transaction.
// Called by the workflow at every state change — this is the append-only
// audit trail in Postgres (tagging table + status_history table).
func SetTxnStatus(ctx context.Context, txnID string, status domain.TxnStatus) error {
	return store.SetTxnStatus(ctx, txnID, status)
}

// FetchThresholds loads the per-tenant confidence thresholds from Postgres.
// Falls back to global config if no tenant-specific row exists.
func FetchThresholds(ctx context.Context, tenantID string) (domain.ConfidenceThreshold, error) {
	return store.FetchThresholds(ctx, tenantID)
}

// WriteReviewQueueItem persists the queued transaction for the accountant UI.
// The UI polls GET /queue for the tenant and renders these rows.
func WriteReviewQueueItem(ctx context.Context, txn domain.Transaction, result domain.TaggingResult) error {
	item := domain.ReviewQueueItem{
		TransactionID:  txn.TransactionID,
		TenantID:       txn.TenantID,
		MerchantName:   txn.MerchantName,
		Amount:         txn.BillingAmount,
		Currency:       txn.BillingCurrency,
		Date:           txn.TransactionDate,
		SuggestedCode:  result.CoACode,
		SuggestedLabel: result.CoALabel,
		Confidence:     result.Confidence,
		Rationale:      result.Rationale,
		Status:         domain.StatusInReview,
		QueuedAt:       time.Now().UTC(),
	}
	return store.WriteReviewQueueItem(ctx, item)
}

func WriteEscalationQueueItem(ctx context.Context, txn domain.Transaction, reason string) error {
	item := domain.ReviewQueueItem{
		TransactionID: txn.TransactionID,
		TenantID:      txn.TenantID,
		MerchantName:  txn.MerchantName,
		Amount:        txn.BillingAmount,
		Currency:      txn.BillingCurrency,
		Date:          txn.TransactionDate,
		Rationale:     reason,
		Status:        domain.StatusEscalated,
		QueuedAt:      time.Now().UTC(),
	}
	return store.WriteReviewQueueItem(ctx, item)
}

func UpdateReviewQueueStatus(ctx context.Context, txnID string, status string) error {
	return store.UpdateReviewQueueStatus(ctx, txnID, status)
}

// WriteCorrectionEvent persists the accountant's decision.
// This is the primary input to the nightly learning loop.
//
// Events are append-only — never updated, never deleted.
// processed = false until the nightly job picks them up.
func WriteCorrectionEvent(ctx context.Context, event domain.CorrectionEvent) error {
	event.ID = uuid.New().String()
	event.CreatedAt = time.Now().UTC()
	event.Processed = false
	return store.WriteCorrectionEvent(ctx, event)
}

// MaybePromoteVendorRule checks if the override count for a vendor+code pair
// has hit the promotion threshold. If so, it writes a hard vendor rule to Postgres.
// Future transactions for this vendor bypass the LLM entirely.
func MaybePromoteVendorRule(ctx context.Context, tenantID, merchantNormalized, coaCode string, threshold int) error {
	count, err := store.CountOverrides(ctx, tenantID, merchantNormalized, coaCode)
	if err != nil {
		return err
	}
	if count < threshold {
		return nil // not yet — stay in RAG-only mode
	}
	return store.UpsertVendorRule(ctx, domain.VendorRule{
		TenantID:           tenantID,
		MerchantNormalized: merchantNormalized,
		CoACode:            coaCode,
		OverrideCount:      count,
		PromotedAt:         time.Now().UTC(),
	})
}
