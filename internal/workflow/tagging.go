package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/reap/cfo-agent/internal/activity"
	"github.com/reap/cfo-agent/internal/domain"
)

const (
	TaskQueueTagging = "tagging-queue"

	// Confidence routing thresholds — fetched dynamically per tenant,
	// these are the global fallbacks used if no tenant config exists.
	DefaultAutoApproveThreshold = 0.90
	DefaultReviewThreshold      = 0.65

	// VendorRulePromotionCount: how many overrides to same code before it
	// becomes a hard rule bypassing LLM entirely.
	VendorRulePromotionCount = 3
)

// TaggingWorkflowInput is the payload that starts a Temporal workflow execution.
type TaggingWorkflowInput struct {
	Event domain.TransactionEvent
}

// TaggingWorkflowResult is the final output stored on the workflow execution.
type TaggingWorkflowResult struct {
	TransactionID string
	FinalStatus   domain.TxnStatus
	CoACode       string
	CoALabel      string
}

// TaggingWorkflow is the durable orchestrator for one transaction.
//
// It replaces the Postgres state-machine + watchdog approach entirely —
// Temporal persists every state transition to its own event store,
// so if the worker crashes mid-flight the workflow replays from the last
// checkpoint. No SELECT FOR UPDATE, no manual lock heartbeats needed.
//
// State transitions mirror your HLD:
//
//	INIT → ENRICHING → TAGGING → AUTO_APPROVE | IN_REVIEW | ESCALATED → SUCCESS
func TaggingWorkflow(ctx workflow.Context, input TaggingWorkflowInput) (*TaggingWorkflowResult, error) {
	txn := input.Event.Txn
	logger := workflow.GetLogger(ctx)
	logger.Info("TaggingWorkflow started", "txn_id", txn.TransactionID, "tenant_id", txn.TenantID)

	// ── 1. Persist INIT state ──────────────────────────────────────────────
	stateCtx := withShortTimeout(ctx, 5*time.Second)
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txn.TransactionID, domain.StatusInit).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	// ── 2. Fetch per-tenant confidence thresholds (dynamic config) ─────────
	var thresholds domain.ConfidenceThreshold
	threshCtx := withShortTimeout(ctx, 5*time.Second)
	if err := workflow.ExecuteActivity(threshCtx, activity.FetchThresholds,
		txn.TenantID).Get(threshCtx, &thresholds); err != nil {
		// Fall back to global defaults — never block on config fetch
		thresholds = domain.ConfidenceThreshold{
			AutoApproveMin: DefaultAutoApproveThreshold,
			ReviewMin:      DefaultReviewThreshold,
		}
	}

	// ── 3. Enrichment — parallel fetch of all context ─────────────────────
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txn.TransactionID, domain.StatusEnriching).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	enrichCtx := withRetryableTimeout(ctx, 10*time.Second, 3)
	var enriched domain.EnrichedContext
	if err := workflow.ExecuteActivity(enrichCtx, activity.EnrichTransaction,
		txn).Get(enrichCtx, &enriched); err != nil {
		// Enrichment failed after retries — escalate, do not drop
		return escalate(ctx, txn.TransactionID, "enrichment_failed")
	}

	// ── 4. Tagging ────────────────────────────────────────────────────────
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txn.TransactionID, domain.StatusTagging).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	tagCtx := withRetryableTimeout(ctx, 30*time.Second, 2)
	var result domain.TaggingResult
	if err := workflow.ExecuteActivity(tagCtx, activity.TagTransaction,
		enriched).Get(tagCtx, &result); err != nil {
		// LLM failed after retries → route to review queue, never silent fail
		logger.Warn("tagging failed, routing to review queue", "txn_id", txn.TransactionID, "error", err)
		return routeToReview(ctx, txn, domain.TaggingResult{
			TransactionID: txn.TransactionID,
			TenantID:      txn.TenantID,
			Confidence:    0,
			Rationale:     "LLM tagging failed — manual review required",
		})
	}

	// ── 5. Confidence router (deterministic — no LLM) ────────────────────
	logger.Info("confidence score", "txn_id", txn.TransactionID, "score", result.Confidence)

	switch {
	case result.Confidence >= thresholds.AutoApproveMin:
		return autoApprove(ctx, txn.TransactionID, result)

	case result.Confidence >= thresholds.ReviewMin:
		return routeToReview(ctx, txn, result)

	default:
		return escalate(ctx, txn.TransactionID, "low_confidence")
	}
}

// ── Route: AUTO_APPROVE ────────────────────────────────────────────────────

func autoApprove(ctx workflow.Context, txnID string, result domain.TaggingResult) (*TaggingWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("auto-approving", "txn_id", txnID, "coa_code", result.CoACode)

	stateCtx := withShortTimeout(ctx, 5*time.Second)
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txnID, domain.StatusAutoApprove).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	// Write correction event (accepted = implicit confirm)
	corrCtx := withShortTimeout(ctx, 5*time.Second)
	if err := workflow.ExecuteActivity(corrCtx, activity.WriteCorrectionEvent,
		domain.CorrectionEvent{
			TransactionID: txnID,
			TenantID:      result.TenantID,
			Action:        domain.ActionAccepted,
			SuggestedCode: result.CoACode,
			FinalCode:     result.CoACode,
		}).Get(corrCtx, nil); err != nil {
		logger.Warn("failed to write correction event", "txn_id", txnID, "error", err)
		// Non-fatal — do not block the approval
	}

	// Mark SUCCESS
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txnID, domain.StatusSuccess).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	return &TaggingWorkflowResult{
		TransactionID: txnID,
		FinalStatus:   domain.StatusSuccess,
		CoACode:       result.CoACode,
		CoALabel:      result.CoALabel,
	}, nil
}

// ── Route: IN_REVIEW ──────────────────────────────────────────────────────
//
// This is where Temporal shines: we can wait indefinitely for an external
// signal (accountant action) using workflow.GetSignalChannel — no polling,
// no Redis expiry, no cron needed. The workflow is durable: if the worker
// restarts, the wait resumes exactly where it left off.

type ReviewSignal struct {
	Action      domain.CorrectionAction
	FinalCode   string
	AccountantID string
}

const SignalReviewAction = "review-action"

func routeToReview(ctx workflow.Context, txn domain.Transaction, result domain.TaggingResult) (*TaggingWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("routing to review queue", "txn_id", txn.TransactionID)

	stateCtx := withShortTimeout(ctx, 5*time.Second)
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txn.TransactionID, domain.StatusInReview).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	// Persist the queue item for the accountant UI
	if err := workflow.ExecuteActivity(stateCtx, activity.WriteReviewQueueItem,
		txn, result).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	// ── Wait for accountant signal (or close-deadline timeout) ─────────────
	// Deadline: 7 days max (end of month close). Per-tenant configurable.
	reviewDeadline := workflow.NewTimer(ctx, 7*24*time.Hour)
	signalCh := workflow.GetSignalChannel(ctx, SignalReviewAction)

	var sig ReviewSignal
	selector := workflow.NewSelector(ctx)

	selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, &sig)
	})
	selector.AddFuture(reviewDeadline, func(_ workflow.Future) {
		// Deadline hit — fail-open: auto-confirm at suggestion
		logger.Warn("review deadline hit, auto-confirming at suggestion", "txn_id", txn.TransactionID)
		sig = ReviewSignal{
			Action:    domain.ActionAccepted,
			FinalCode: result.CoACode,
		}
	})

	selector.Select(ctx)

	// Process the accountant's decision
	corrCtx := withShortTimeout(ctx, 5*time.Second)
	if err := workflow.ExecuteActivity(corrCtx, activity.WriteCorrectionEvent,
		domain.CorrectionEvent{
			TransactionID: txn.TransactionID,
			TenantID:      txn.TenantID,
			Action:        sig.Action,
			SuggestedCode: result.CoACode,
			FinalCode:     sig.FinalCode,
			AccountantID:  sig.AccountantID,
		}).Get(corrCtx, nil); err != nil {
		logger.Warn("failed to write correction event", "error", err)
	}

	if sig.Action == domain.ActionFlagged {
		return escalate(ctx, txn.TransactionID, "accountant_flagged")
	}

	// Promote vendor rule if override threshold hit
	if sig.Action == domain.ActionOverridden {
		promCtx := withShortTimeout(ctx, 5*time.Second)
		workflow.ExecuteActivity(promCtx, activity.MaybePromoteVendorRule,
			txn.TenantID, txn.MerchantNormalizedName, sig.FinalCode, VendorRulePromotionCount).Get(promCtx, nil)
	}

	finalStatus := domain.StatusSuccess
	if err := workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txn.TransactionID, finalStatus).Get(stateCtx, nil); err != nil {
		return nil, err
	}

	finalCode := sig.FinalCode
	if finalCode == "" {
		finalCode = result.CoACode
	}

	return &TaggingWorkflowResult{
		TransactionID: txn.TransactionID,
		FinalStatus:   finalStatus,
		CoACode:       finalCode,
	}, nil
}

// ── Route: ESCALATED ──────────────────────────────────────────────────────

func escalate(ctx workflow.Context, txnID string, reason string) (*TaggingWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Warn("escalating transaction", "txn_id", txnID, "reason", reason)

	stateCtx := withShortTimeout(ctx, 5*time.Second)
	workflow.ExecuteActivity(stateCtx, activity.SetTxnStatus,
		txnID, domain.StatusEscalated).Get(stateCtx, nil)

	return &TaggingWorkflowResult{
		TransactionID: txnID,
		FinalStatus:   domain.StatusEscalated,
	}, nil
}

// ── Helpers ────────────────────────────────────────────────────────────────

func withShortTimeout(ctx workflow.Context, d time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: d,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	})
}

func withRetryableTimeout(ctx workflow.Context, d time.Duration, maxAttempts int32) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: d,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    maxAttempts,
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
		},
	})
}
