package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/reap/cfo-agent/internal/domain"
	"github.com/reap/cfo-agent/internal/llm"
)

// TagTransaction is the Temporal activity that calls the LLM.
// If a hard vendor rule exists it short-circuits the LLM entirely.
//
// Key guarantee: the returned CoACode is always in the tenant's CoA.
// If the LLM returns an invalid code we retry with a stricter prompt once,
// then return an error (Temporal retries the whole activity).
func TagTransaction(ctx context.Context, ec domain.EnrichedContext) (domain.TaggingResult, error) {
	txn := ec.Txn

	// ── Hard vendor rule — bypass LLM ────────────────────────────────────
	if ec.HardRuleExists {
		label := findLabel(ec.CoAEntries, ec.HardRuleCoACode)
		return domain.TaggingResult{
			TransactionID: txn.TransactionID,
			TenantID:      txn.TenantID,
			CoACode:       ec.HardRuleCoACode,
			CoALabel:      label,
			Confidence:    0.99,
			Rationale:     fmt.Sprintf("hard vendor rule: %s always → %s", txn.MerchantNormalizedName, ec.HardRuleCoACode),
			Source:        "vendor_rule",
		}, nil
	}

	// ── Build the prompt ──────────────────────────────────────────────────
	prompt := buildPrompt(ec)

	// ── LLM call ──────────────────────────────────────────────────────────
	raw, err := llm.Complete(ctx, prompt)
	if err != nil {
		return domain.TaggingResult{}, fmt.Errorf("llm call failed: %w", err)
	}
	log.Printf("[tag] llm raw response txn_id=%s body=%s", txn.TransactionID, raw)

	// ── Parse structured output ───────────────────────────────────────────
	result, err := parseAndValidate(raw, ec)
	if err != nil {
		// First parse failure — retry with a stricter prompt that enumerates codes
		log.Printf("[tag] first parse failed txn_id=%s err=%v", txn.TransactionID, err)
		strictPrompt := buildStrictPrompt(ec)
		raw2, err2 := llm.Complete(ctx, strictPrompt)
		if err2 != nil {
			return domain.TaggingResult{}, fmt.Errorf("llm retry failed: %w", err2)
		}
		log.Printf("[tag] llm strict raw response txn_id=%s body=%s", txn.TransactionID, raw2)
		result, err = parseAndValidate(raw2, ec)
		if err == nil {
			log.Printf("[tag] parsed strict result txn_id=%s coa_code=%s confidence=%.3f", txn.TransactionID, result.CoACode, result.Confidence)
		}
		if err != nil {
			return domain.TaggingResult{}, fmt.Errorf("llm output invalid after retry: %w", err)
		}
	}

	source := "llm"
	if len(ec.RAGNeighbours) == 0 {
		source = "cold_start"
	}
	result.Source = source
	log.Printf("[tag] parsed result txn_id=%s coa_code=%s confidence=%.3f source=%s", txn.TransactionID, result.CoACode, result.Confidence, result.Source)

	return result, nil
}

// ── Prompt builder ─────────────────────────────────────────────────────────
//
// The prompt has four mandatory sections in this order:
//  1. Role + closed output constraint (CoA codes)
//  2. Historical few-shot examples from RAG (or cold-start notice)
//  3. Current transaction facts
//  4. Output schema

func buildPrompt(ec domain.EnrichedContext) string {
	var b strings.Builder

	// Section 1: role + closed output constraint
	b.WriteString("You are a transaction coder for a finance team.\n")
	b.WriteString("Tag the transaction below with exactly one account code from this list.\n\n")
	b.WriteString("VALID ACCOUNT CODES (you MUST pick one of these):\n")
	for _, e := range ec.CoAEntries {
		fmt.Fprintf(&b, "  %s: %s\n", e.Code, e.Label)
	}
	b.WriteString("\n")

	// Section 2: RAG few-shot examples
	if len(ec.RAGNeighbours) == 0 {
		b.WriteString("HISTORICAL CONTEXT: No past transactions available for this tenant.\n")
		b.WriteString("Reason purely from the account descriptions and transaction details.\n\n")
	} else {
		b.WriteString("SIMILAR PAST TRANSACTIONS (confirmed by accountant):\n")
		for _, n := range ec.RAGNeighbours {
			fmt.Fprintf(&b, "  [%.2f] %q → %s (%s) [%s]\n",
				n.Similarity, n.TransactionDesc, n.CoACode, n.CoALabel, n.Source)
		}
		b.WriteString("\n")
	}

	// Section 3: current transaction
	txn := ec.Txn
	b.WriteString("TRANSACTION TO TAG:\n")
	fmt.Fprintf(&b, "  merchant: %s\n", txn.MerchantNormalizedName)
	fmt.Fprintf(&b, "  mcc: %s (%s)\n", txn.MCC, txn.MCCDescription)
	fmt.Fprintf(&b, "  amount: %s %.2f (billed: %s %.2f)\n",
		txn.Currency, txn.Amount, txn.BillingCurrency, txn.BillingAmount)
	fmt.Fprintf(&b, "  description: %s\n", txn.Description)
	fmt.Fprintf(&b, "  receipt: %s\n", ec.ReceiptSummary)
	if len(ec.CategorySignals) > 0 {
		fmt.Fprintf(&b, "  signals: %s\n", strings.Join(ec.CategorySignals, ", "))
	}
	b.WriteString("\n")

	// Section 4: output schema
	b.WriteString("Respond with ONLY a JSON object, no markdown, no preamble:\n")
	b.WriteString(`{"coa_code": "<code>", "confidence": <0.0-1.0>, "rationale": "<max 25 words citing evidence>"}`)
	b.WriteString("\n")

	return b.String()
}

// buildStrictPrompt is used on retry — it enumerates codes explicitly
// in the JSON schema to reduce hallucination.
func buildStrictPrompt(ec domain.EnrichedContext) string {
	codes := make([]string, len(ec.CoAEntries))
	for i, e := range ec.CoAEntries {
		codes[i] = fmt.Sprintf("%q", e.Code)
	}
	base := buildPrompt(ec)
	return base + fmt.Sprintf(
		"\nIMPORTANT: coa_code MUST be one of: [%s]. Do not invent codes.\n",
		strings.Join(codes, ", "),
	)
}

// ── Output parser + validator ──────────────────────────────────────────────

type llmOutput struct {
	CoACode    string   `json:"coa_code"`
	Confidence *float64 `json:"confidence"`
	Rationale  string   `json:"rationale"`
}

func parseAndValidate(raw string, ec domain.EnrichedContext) (domain.TaggingResult, error) {
	// Strip any accidental markdown fences
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")

	var out llmOutput
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return domain.TaggingResult{}, fmt.Errorf("json parse failed: %w", err)
	}

	if out.Confidence == nil {
		return domain.TaggingResult{}, fmt.Errorf("missing confidence field")
	}
	if strings.TrimSpace(out.Rationale) == "" {
		return domain.TaggingResult{}, fmt.Errorf("missing rationale field")
	}

	// Hard validation: code must be in the tenant's CoA
	label := findLabel(ec.CoAEntries, out.CoACode)
	if label == "" {
		return domain.TaggingResult{}, fmt.Errorf("coa_code %q not in tenant CoA", out.CoACode)
	}

	confidence := *out.Confidence
	if confidence < 0 {
		confidence = 0
	}
	if confidence > 1 {
		confidence = 1
	}

	// Truncate rationale to 25 words
	rationale := truncateWords(out.Rationale, 25)

	return domain.TaggingResult{
		TransactionID: ec.Txn.TransactionID,
		TenantID:      ec.Txn.TenantID,
		CoACode:       out.CoACode,
		CoALabel:      label,
		Confidence:    confidence,
		Rationale:     rationale,
	}, nil
}

// ── Helpers ────────────────────────────────────────────────────────────────

func findLabel(entries []domain.CoAEntry, code string) string {
	for _, e := range entries {
		if e.Code == code {
			return e.Label
		}
	}
	return ""
}

func truncateWords(s string, maxWords int) string {
	words := strings.Fields(s)
	if len(words) <= maxWords {
		return s
	}
	return strings.Join(words[:maxWords], " ") + "…"
}
