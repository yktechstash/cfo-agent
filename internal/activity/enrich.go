package activity

import (
	"context"
	"fmt"
	"strings"

	"github.com/reap/cfo-agent/internal/domain"
	"github.com/reap/cfo-agent/internal/store"
)

// EnrichTransaction is the Temporal activity that assembles EnrichedContext
// from the raw transaction. It runs three fetches in parallel using goroutines
// (safe inside a Temporal activity — goroutines are allowed, just not in workflows).
//
// Outputs: EnrichedContext passed directly to TagTransaction activity.
func EnrichTransaction(ctx context.Context, txn domain.Transaction) (domain.EnrichedContext, error) {
	type result[T any] struct {
		val T
		err error
	}

	// ── Parallel fetch ────────────────────────────────────────────────────
	receiptCh := make(chan result[string], 1)
	signalsCh := make(chan result[[]string], 1)
	ragCh := make(chan result[[]domain.RAGNeighbour], 1)
	coaCh := make(chan result[[]domain.CoAEntry], 1)
	ruleCh := make(chan result[*domain.VendorRule], 1)

	go func() {
		summary, err := fetchReceiptSummary(ctx, txn)
		receiptCh <- result[string]{summary, err}
	}()
	go func() {
		signals := deriveSignals(txn)
		signalsCh <- result[[]string]{signals, nil}
	}()
	go func() {
		neighbours, err := store.FetchRAGNeighbours(ctx, txn.TenantID, buildEmbedInput(txn), 5)
		ragCh <- result[[]domain.RAGNeighbour]{neighbours, err}
	}()
	go func() {
		entries, err := store.FetchCoAEntries(ctx, txn.TenantID)
		coaCh <- result[[]domain.CoAEntry]{entries, err}
	}()
	go func() {
		rule, err := store.FetchVendorRule(ctx, txn.TenantID, txn.MerchantNormalizedName)
		ruleCh <- result[*domain.VendorRule]{rule, err}
	}()

	receiptResult := <-receiptCh
	signalsResult := <-signalsCh
	ragResult := <-ragCh
	coaResult := <-coaCh
	ruleResult := <-ruleCh

	// CoA is mandatory — fail if missing
	if coaResult.err != nil {
		return domain.EnrichedContext{}, fmt.Errorf("failed to fetch CoA for tenant %s: %w", txn.TenantID, coaResult.err)
	}

	// RAG failure is non-fatal — cold start path
	neighbours := ragResult.val
	if ragResult.err != nil {
		neighbours = []domain.RAGNeighbour{} // empty = cold start
	}

	// Receipt failure is non-fatal — absence is communicated explicitly
	receiptSummary := receiptResult.val
	if receiptResult.err != nil || receiptSummary == "" {
		receiptSummary = "no receipt"
	}

	// Hard vendor rule
	hardRuleExists := false
	hardRuleCode := ""
	if ruleResult.err == nil && ruleResult.val != nil {
		hardRuleExists = true
		hardRuleCode = ruleResult.val.CoACode
	}

	return domain.EnrichedContext{
		Txn:             txn,
		ReceiptSummary:  receiptSummary,
		CategorySignals: signalsResult.val,
		RAGNeighbours:   neighbours,
		CoAEntries:      coaResult.val,
		HardRuleExists:  hardRuleExists,
		HardRuleCoACode: hardRuleCode,
	}, nil
}

// fetchReceiptSummary extracts the key fields from the OCR receipt and
// returns a compact natural-language summary for the LLM prompt.
// If no receipt is linked or OCR fails, it returns an empty string.
func fetchReceiptSummary(ctx context.Context, txn domain.Transaction) (string, error) {
	if txn.ReceiptStatus != "matched" || txn.ReceiptID == "" {
		return "no receipt", nil
	}

	receipt, err := store.FetchReceipt(ctx, txn.ReceiptID)
	if err != nil {
		return "", err
	}

	// Build a compact summary — we do NOT dump raw OCR text into the prompt.
	// The LLM sees structured facts, not raw noise.
	parts := []string{
		fmt.Sprintf("vendor: %s", receipt.VendorNormalised),
		fmt.Sprintf("amount: %s %.2f", receipt.Currency, receipt.Total),
	}
	if receipt.TaxType != "" {
		parts = append(parts, fmt.Sprintf("tax type: %s", receipt.TaxType))
	}
	if len(receipt.LineItems) > 0 {
		items := make([]string, 0, len(receipt.LineItems))
		for _, li := range receipt.LineItems {
			items = append(items, li.Description)
		}
		parts = append(parts, fmt.Sprintf("line items: %s", strings.Join(items, ", ")))
	}
	if !receipt.AmountVerified {
		parts = append(parts, "NOTE: receipt amount does not match card charge — pre-tax subtotal used")
	}

	return strings.Join(parts, "; "), nil
}

// deriveSignals extracts deterministic category hints from the transaction
// before the LLM sees it. These hints boost confidence scoring downstream.
func deriveSignals(txn domain.Transaction) []string {
	signals := []string{}

	// MCC-based signals
	switch txn.MCC {
	case "7372", "5734", "4816":
		signals = append(signals, "SaaS", "software")
	case "7011":
		signals = append(signals, "hotel", "accommodation")
	case "4121":
		signals = append(signals, "ride_hailing", "local_transport")
		if strings.Contains(strings.ToLower(txn.Description), "airport") {
			signals = append(signals, "airport_route")
		}
	case "5812", "5814":
		signals = append(signals, "food_beverage")
	case "4511":
		signals = append(signals, "airline", "travel")
	}

	// FX signal — multi-currency txn is relevant for some CoA categories
	if txn.Currency != txn.BillingCurrency {
		signals = append(signals, "multi_currency")
	}

	// Receipt status signal — absence is information
	if txn.ReceiptStatus != "matched" {
		signals = append(signals, "no_receipt")
	}

	return signals
}

// buildEmbedInput constructs the string we embed for vector search.
// Must match exactly what is stored in the RAG namespace for similarity to work.
func buildEmbedInput(txn domain.Transaction) string {
	return fmt.Sprintf("%s %s %s %.0f %s",
		txn.MerchantNormalizedName,
		txn.MCCDescription,
		txn.Description,
		txn.Amount,
		txn.Currency,
	)
}
