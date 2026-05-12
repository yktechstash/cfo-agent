-- Migration: 001_initial_schema.sql
-- Run with: psql $POSTGRES_DSN -f migrations/001_initial_schema.sql

-- ── Extensions ────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;  -- pgvector

-- ── Transactions (raw inbound) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id            TEXT PRIMARY KEY,
    tenant_id                 TEXT NOT NULL,
    source                    TEXT NOT NULL,         -- reap_card | reap_pay
    merchant_name             TEXT NOT NULL,
    merchant_normalized_name  TEXT NOT NULL,
    mcc                       TEXT NOT NULL,
    mcc_description           TEXT NOT NULL,
    amount                    NUMERIC(18,4) NOT NULL,
    currency                  TEXT NOT NULL,
    billing_amount            NUMERIC(18,4),
    billing_currency          TEXT,
    fx_rate                   NUMERIC(10,6),
    transaction_date          DATE NOT NULL,
    posted_at                 TIMESTAMPTZ,
    card_last4                TEXT,
    description               TEXT,
    receipt_id                TEXT,
    receipt_status            TEXT DEFAULT 'unmatched',
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_transactions_tenant ON transactions(tenant_id);

-- ── Tagging state machine ─────────────────────────────────────────────────
-- One row per transaction, status updated on every transition.
CREATE TABLE IF NOT EXISTS tagging (
    transaction_id  TEXT PRIMARY KEY REFERENCES transactions(transaction_id),
    status          TEXT NOT NULL DEFAULT 'INIT',
    coa_code        TEXT,
    coa_label       TEXT,
    confidence      NUMERIC(4,3),
    rationale       TEXT,
    source          TEXT,        -- llm | vendor_rule | cold_start
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Status history (append-only audit trail) ──────────────────────────────
CREATE TABLE IF NOT EXISTS status_history (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  TEXT NOT NULL REFERENCES transactions(transaction_id),
    status          TEXT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_status_history_txn ON status_history(transaction_id);

-- ── Per-tenant chart of accounts ─────────────────────────────────────────
-- Flat structure as agreed: { "6100": "Cloud Infrastructure", ... }
-- Updated via API when tenant changes their CoA.
CREATE TABLE IF NOT EXISTS coa_schema (
    tenant_id   TEXT NOT NULL,
    code        TEXT NOT NULL,
    label       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, code)
);

-- ── Confidence thresholds (dynamic, per-tenant) ───────────────────────────
-- tenant_id = '' is the global fallback row.
CREATE TABLE IF NOT EXISTS confidence_thresholds (
    tenant_id         TEXT PRIMARY KEY,
    auto_approve_min  NUMERIC(4,3) NOT NULL DEFAULT 0.90,
    review_min        NUMERIC(4,3) NOT NULL DEFAULT 0.65,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Insert global defaults
INSERT INTO confidence_thresholds (tenant_id, auto_approve_min, review_min)
VALUES ('', 0.90, 0.65)
ON CONFLICT (tenant_id) DO NOTHING;

-- ── Vendor rules (hard overrides, promoted from RAG corrections) ──────────
CREATE TABLE IF NOT EXISTS vendor_rules (
    tenant_id             TEXT NOT NULL,
    merchant_normalized   TEXT NOT NULL,
    coa_code              TEXT NOT NULL,
    override_count        INT NOT NULL DEFAULT 0,
    promoted_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, merchant_normalized)
);

-- ── Review queue ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS review_queue (
    transaction_id   TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    merchant_name    TEXT NOT NULL,
    amount           NUMERIC(18,4) NOT NULL,
    currency         TEXT NOT NULL,
    transaction_date TEXT,
    suggested_code   TEXT,
    suggested_label  TEXT,
    confidence       NUMERIC(4,3),
    rationale        TEXT,
    status           TEXT NOT NULL DEFAULT 'IN_REVIEW',
    queued_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_review_queue_tenant ON review_queue(tenant_id, status);

-- ── Correction events (append-only learning signal) ───────────────────────
CREATE TABLE IF NOT EXISTS correction_events (
    id               TEXT PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id   TEXT NOT NULL,
    tenant_id        TEXT NOT NULL,
    action           TEXT NOT NULL,   -- ACCEPTED | OVERRIDDEN | FLAGGED
    suggested_code   TEXT,
    final_code       TEXT,
    accountant_id    TEXT,
    merchant_normalized TEXT,         -- denormalized for vendor rule promotion query
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed        BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_correction_events_tenant_unprocessed
    ON correction_events(tenant_id, processed)
    WHERE processed = FALSE;

-- ── RAG vector store (pgvector) ───────────────────────────────────────────
-- One row per confirmed transaction per tenant.
-- tenant_id in WHERE clause = per-tenant namespace isolation.
-- Dimension 1536 = text-embedding-3-small. Change to 3072 for large.
CREATE TABLE IF NOT EXISTS txn_vectors (
    transaction_id  TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    txn_desc        TEXT NOT NULL,     -- the string we embedded
    coa_code        TEXT NOT NULL,
    coa_label       TEXT NOT NULL,
    source          TEXT NOT NULL,     -- accepted | overridden
    embedding       vector(1536) NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- ivfflat index for approximate kNN — tune lists= as corpus grows
-- For < 1M vectors: lists = sqrt(rows). Re-run VACUUM ANALYZE after bulk loads.
CREATE INDEX IF NOT EXISTS idx_txn_vectors_tenant_embedding
    ON txn_vectors USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_txn_vectors_tenant
    ON txn_vectors(tenant_id);

-- ── Receipt OCR ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS receipt_ocr (
    receipt_id         TEXT PRIMARY KEY,
    transaction_id     TEXT REFERENCES transactions(transaction_id),
    vendor_normalised  TEXT,
    currency           TEXT,
    total              NUMERIC(18,4),
    subtotal           NUMERIC(18,4),
    tax_type           TEXT,           -- GST | VAT | PPN | none
    tax_amount         NUMERIC(18,4),
    amount_verified    BOOLEAN DEFAULT FALSE,
    original_language  TEXT DEFAULT 'en',
    raw_text           TEXT,           -- full OCR dump for audit
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Nightly learning loop log (idempotency) ───────────────────────────────
CREATE TABLE IF NOT EXISTS learning_run_log (
    tenant_id         TEXT NOT NULL,
    run_date          DATE NOT NULL,
    events_processed  INT NOT NULL DEFAULT 0,
    ran_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, run_date)
);
