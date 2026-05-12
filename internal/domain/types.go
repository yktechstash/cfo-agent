package domain

import "time"

// TxnStatus is the state machine for every transaction moving through the pipeline.
// Postgres column: tagging.status
type TxnStatus string

const (
	StatusInit          TxnStatus = "INIT"
	StatusEnriching     TxnStatus = "ENRICHING"
	StatusTagging       TxnStatus = "TAGGING"
	StatusAutoApprove   TxnStatus = "AUTO_APPROVE"
	StatusInReview      TxnStatus = "IN_REVIEW"
	StatusEscalated     TxnStatus = "ESCALATED"
	StatusSuccess       TxnStatus = "SUCCESS"
)

// TransactionEvent is the raw event from the Kafka / SQS stream.
type TransactionEvent struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"`
	EventTime time.Time   `json:"event_time"`
	Txn       Transaction `json:"transaction"`
}

// Transaction is the core entity — one card spend or bill pay.
type Transaction struct {
	TransactionID          string    `json:"transaction_id"`
	TenantID               string    `json:"tenant_id"`
	Source                 string    `json:"source"` // "reap_card" | "reap_pay"
	Status                 string    `json:"status"`
	MerchantName           string    `json:"merchant_name"`
	MerchantNormalizedName string    `json:"merchant_normalized_name"`
	MCC                    string    `json:"mcc"`
	MCCDescription         string    `json:"mcc_description"`
	Amount                 float64   `json:"amount"`
	Currency               string    `json:"currency"`
	BillingAmount          float64   `json:"billing_amount"`
	BillingCurrency        string    `json:"billing_currency"`
	FXRate                 float64   `json:"fx_rate"`
	TransactionDate        string    `json:"transaction_date"`
	PostedAt               time.Time `json:"posted_at"`
	CardLast4              string    `json:"card_last4"`
	Description            string    `json:"description"`
	ReceiptID              string    `json:"receipt_id"`
	ReceiptStatus          string    `json:"receipt_status"` // "matched" | "unmatched" | "pending"
}

// CoAEntry is one account code in a tenant's chart of accounts.
// Stored flat as per the agreed assumption:
//
//	{ "6100": "Cloud Infrastructure", "6200": "Software Subscriptions" }
type CoAEntry struct {
	Code  string `json:"code"`
	Label string `json:"label"`
}

// VendorRule is a hard deterministic override — promoted from RAG after N corrections.
// Once a rule exists, the tagging worker bypasses the LLM entirely.
type VendorRule struct {
	TenantID             string    `json:"tenant_id"`
	MerchantNormalized   string    `json:"merchant_normalized"`
	CoACode              string    `json:"coa_code"`
	OverrideCount        int       `json:"override_count"`
	PromotedAt           time.Time `json:"promoted_at"`
}

// TaggingResult is what the LLM (or rule engine) returns for a transaction.
type TaggingResult struct {
	TransactionID string  `json:"transaction_id"`
	TenantID      string  `json:"tenant_id"`
	CoACode       string  `json:"coa_code"`
	CoALabel      string  `json:"coa_label"`
	Confidence    float64 `json:"confidence"`
	Rationale     string  `json:"rationale"`  // ≤25 words citing evidence
	Source        string  `json:"source"`     // "llm" | "vendor_rule" | "cold_start"
}

// CorrectionAction is the accountant's decision on a queued transaction.
type CorrectionAction string

const (
	ActionAccepted  CorrectionAction = "ACCEPTED"
	ActionOverridden CorrectionAction = "OVERRIDDEN"
	ActionFlagged   CorrectionAction = "FLAGGED"
)

// CorrectionEvent is written every time an accountant acts on a queued transaction.
// This is the primary learning signal for the nightly loop.
type CorrectionEvent struct {
	ID              string           `json:"id"`
	TransactionID   string           `json:"transaction_id"`
	TenantID        string           `json:"tenant_id"`
	Action          CorrectionAction `json:"action"`
	SuggestedCode   string           `json:"suggested_code"`
	FinalCode       string           `json:"final_code"`
	AccountantID    string           `json:"accountant_id"`
	CreatedAt       time.Time        `json:"created_at"`
	Processed       bool             `json:"processed"` // set true by nightly learning loop
}

// RAGNeighbour is a single result from the vector store kNN query.
type RAGNeighbour struct {
	TransactionDesc string  `json:"transaction_desc"`
	CoACode         string  `json:"coa_code"`
	CoALabel        string  `json:"coa_label"`
	Similarity      float64 `json:"similarity"`
	Source          string  `json:"source"` // "accepted" | "overridden"
}

// EnrichedContext is assembled by the enrichment activity and passed to the tagging activity.
type EnrichedContext struct {
	Txn              Transaction    `json:"txn"`
	ReceiptSummary   string         `json:"receipt_summary"`   // extracted fields or "no receipt"
	CategorySignals  []string       `json:"category_signals"`  // e.g. ["airport_route", "SaaS"]
	RAGNeighbours    []RAGNeighbour `json:"rag_neighbours"`
	CoAEntries       []CoAEntry     `json:"coa_entries"`
	HardRuleExists   bool           `json:"hard_rule_exists"`
	HardRuleCoACode  string         `json:"hard_rule_coa_code"`
}

// ConfidenceThreshold holds the per-tenant (or global fallback) routing thresholds.
// Stored in Postgres and fetched at workflow start — dynamic, not hardcoded.
type ConfidenceThreshold struct {
	TenantID       string  `json:"tenant_id"` // empty = global default
	AutoApproveMin float64 `json:"auto_approve_min"` // >= this → AUTO_APPROVE
	ReviewMin      float64 `json:"review_min"`       // >= this → IN_REVIEW; below → ESCALATED
}

// ReviewQueueItem is what the accountant UI fetches for a single transaction.
type ReviewQueueItem struct {
	TransactionID   string         `json:"transaction_id"`
	TenantID        string         `json:"tenant_id"`
	MerchantName    string         `json:"merchant_name"`
	Amount          float64        `json:"amount"`
	Currency        string         `json:"currency"`
	Date            string         `json:"date"`
	ContextTags     []string       `json:"context_tags"`
	SuggestedCode   string         `json:"suggested_code"`
	SuggestedLabel  string         `json:"suggested_label"`
	Confidence      float64        `json:"confidence"`
	Rationale       string         `json:"rationale"`
	CoAOptions      []CoAEntry     `json:"coa_options"`
	Status          TxnStatus      `json:"status"`
	QueuedAt        time.Time      `json:"queued_at"`
}
