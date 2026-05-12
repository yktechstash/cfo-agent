# Reap CFO Agent — Transaction Auto-Tagging

Go implementation of Workflow 1 from the Reap CFO Agent take-home.

## Architecture decisions

### Why Temporal (not a Postgres state machine)

Your HLD correctly identified the need for a state machine. The natural Go implementation is Temporal:

- **Durability**: every state transition is persisted to Temporal's event store automatically. If the worker crashes mid-tagging, the workflow replays from the last checkpoint. No SELECT FOR UPDATE, no lock heartbeats, no watchdog cron.
- **Signals**: the review queue wait (`IN_REVIEW → CONFIRMED`) is implemented as `workflow.GetSignalChannel` — the workflow parks durably until the accountant signals it via POST /review. No polling, no Redis expiry.
- **Retries**: LLM call failures get typed retry policies (exponential backoff for network errors, stricter prompt on parse failure) without any retry infrastructure code.
- **Observability**: Temporal UI shows the full state history of every workflow execution out of the box.

### States (from your HLD, verbatim)

```
INIT → ENRICHING → TAGGING → AUTO_APPROVE | IN_REVIEW | ESCALATED → SUCCESS
```

### Confidence routing (deterministic, no LLM)

```
>= auto_approve_min (default 0.90) → AUTO_APPROVE → SUCCESS
>= review_min       (default 0.65) → IN_REVIEW (accountant queue)
<  review_min                       → ESCALATED (flag, no suggestion shown)
```

Thresholds stored in `confidence_thresholds` table — per-tenant or global fallback. Dynamic, not hardcoded.

### Per-tenant CoA isolation

Every RAG query, CoA fetch, vendor rule lookup, and review queue query is filtered by `tenant_id`. There is no shared namespace. Same vendor (e.g. AWS) can map to different CoA codes per tenant.

### Hard vendor rules

After `VendorRulePromotionCount` (default 3) overrides of the same vendor to the same code, `MaybePromoteVendorRule` writes a hard rule. Future transactions for that vendor skip the LLM entirely — confidence = 0.99.

### Silent error prevention

- LLM output is validated against the tenant's CoA before acceptance (set membership check)
- Invalid code → retry with stricter prompt → still invalid → activity returns error → Temporal retries → exhausted → route to `IN_REVIEW`, never auto-post
- Absence of receipt is communicated explicitly to the LLM ("no receipt"), not silently omitted

## Directory structure

```
cmd/
  worker/main.go     — Temporal worker (registers workflow + activities)
  api/main.go        — Gin HTTP API server
internal/
  domain/types.go    — all types, zero business logic
  workflow/tagging.go — Temporal workflow (state machine + signal wait)
  activity/
    enrich.go        — parallel fetch: receipt, vendor, RAG, CoA
    tag.go           — context assembler + LLM call + output validation
    state.go         — SetTxnStatus, WriteCorrectionEvent, MaybePromoteVendorRule
    learning_loop.go — nightly idempotent RAG refresh
  store/postgres.go  — pgx + pgvector queries
  llm/client.go      — Anthropic SDK wrapper
  api/server.go      — Gin handlers
  config/config.go   — env config
migrations/
  001_initial_schema.sql
```

## Running locally

### Prerequisites
- Go 1.22+
- Docker (for Temporal + Postgres)
- Anthropic API key

### Start dependencies

```bash
# Temporal (includes its own Postgres)
docker run --rm -d -p 7233:7233 temporalio/auto-setup:1.24

# Postgres with pgvector
docker run --rm -d \
  -e POSTGRES_USER=reap \
  -e POSTGRES_PASSWORD=reap \
  -e POSTGRES_DB=cfo_agent \
  -p 5432:5432 \
  pgvector/pgvector:pg16
```

### Run migrations

```bash
psql "postgres://reap:reap@localhost:5432/cfo_agent?sslmode=disable" \
  -f migrations/001_initial_schema.sql
```

### Start the worker

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export POSTGRES_DSN="postgres://reap:reap@localhost:5432/cfo_agent?sslmode=disable"

go run ./cmd/worker
```

### Start the API server

```bash
go run ./cmd/api
```

### Reset DB + seed 10 test transactions

`tenant_id` is the namespace for a company/account. All CoA entries, review queue items, vendor rules, and RAG vectors are partitioned by `tenant_id`. For local testing you can just use `tenant_1` consistently.

#### Option A: wipe everything (recommended)

This deletes the Postgres volume and re-runs migrations on next start.

```bash
docker compose down -v
docker compose up -d
```

#### Option B: clear tables + insert seed data

This keeps the DB volume but truncates data and inserts a ready-to-test dataset.

```bash
docker compose exec db psql -U reap -d cfo_agent -v ON_ERROR_STOP=1 -c "
BEGIN;

TRUNCATE
  txn_vectors,
  correction_events,
  review_queue,
  vendor_rules,
  learning_run_log,
  status_history,
  tagging,
  receipt_ocr,
  transactions,
  coa_schema,
  confidence_thresholds
RESTART IDENTITY CASCADE;

INSERT INTO confidence_thresholds (tenant_id, auto_approve_min, review_min)
VALUES ('', 0.90, 0.65)
ON CONFLICT (tenant_id) DO UPDATE
SET auto_approve_min = EXCLUDED.auto_approve_min,
    review_min = EXCLUDED.review_min,
    updated_at = NOW();

INSERT INTO confidence_thresholds (tenant_id, auto_approve_min, review_min)
VALUES ('tenant_1', 0.90, 0.65)
ON CONFLICT (tenant_id) DO UPDATE
SET auto_approve_min = EXCLUDED.auto_approve_min,
    review_min = EXCLUDED.review_min,
    updated_at = NOW();

INSERT INTO coa_schema (tenant_id, code, label) VALUES
  ('tenant_1','6100','Cloud Infrastructure'),
  ('tenant_1','6200','Software Subscriptions'),
  ('tenant_1','6300','Travel'),
  ('tenant_1','6400','Meals & Entertainment'),
  ('tenant_1','6500','Shipping & Courier'),
  ('tenant_1','6600','Rent & Coworking')
ON CONFLICT (tenant_id, code) DO UPDATE
SET label = EXCLUDED.label,
    updated_at = NOW();

INSERT INTO transactions (
  transaction_id, tenant_id, source,
  merchant_name, merchant_normalized_name,
  mcc, mcc_description,
  amount, currency,
  billing_amount, billing_currency,
  fx_rate,
  transaction_date, posted_at,
  card_last4, description,
  receipt_id, receipt_status
) VALUES
  ('txn_001','tenant_1','reap_card','Amazon Web Services','amazon web services','7372','Computer programming / data processing',12.34,'USD',12.34,'USD',1.0000,'2026-05-01','2026-05-01T10:00:00Z','1234','AWS monthly subscription','','unmatched'),
  ('txn_002','tenant_1','reap_card','Slack','slack','7372','Computer programming / data processing',8.00,'USD',8.00,'USD',1.0000,'2026-05-02','2026-05-02T10:00:00Z','1234','Slack workspace seats','','unmatched'),
  ('txn_003','tenant_1','reap_card','Google Cloud','google cloud','7372','Computer programming / data processing',22.10,'USD',22.10,'USD',1.0000,'2026-05-03','2026-05-03T10:00:00Z','1234','GCP project usage','','unmatched'),
  ('txn_004','tenant_1','reap_card','Uber','uber','4121','Taxicabs and limousines',18.75,'USD',18.75,'USD',1.0000,'2026-05-04','2026-05-04T10:00:00Z','1234','Ride to client meeting','','unmatched'),
  ('txn_005','tenant_1','reap_card','Hilton Hotels','hilton','7011','Lodging - hotels, motels, resorts',189.00,'USD',189.00,'USD',1.0000,'2026-05-05','2026-05-05T10:00:00Z','1234','Hotel stay - business travel','','unmatched'),
  ('txn_006','tenant_1','reap_card','Delta Air Lines','delta','4511','Airlines, air carriers',420.50,'USD',420.50,'USD',1.0000,'2026-05-06','2026-05-06T10:00:00Z','1234','Flight for conference','','unmatched'),
  ('txn_007','tenant_1','reap_card','Starbucks','starbucks','5814','Fast food restaurants',6.45,'USD',6.45,'USD',1.0000,'2026-05-07','2026-05-07T10:00:00Z','1234','Coffee with client','','unmatched'),
  ('txn_008','tenant_1','reap_card','WeWork','wework','6513','Real estate agents and managers',350.00,'USD',350.00,'USD',1.0000,'2026-05-08','2026-05-08T10:00:00Z','1234','Coworking monthly pass','','unmatched'),
  ('txn_009','tenant_1','reap_card','FedEx','fedex','4215','Courier services',24.99,'USD',24.99,'USD',1.0000,'2026-05-09','2026-05-09T10:00:00Z','1234','Document shipping','','unmatched'),
  ('txn_010','tenant_1','reap_card','Stripe','stripe','7399','Business services',29.00,'USD',29.00,'USD',1.0000,'2026-05-10','2026-05-10T10:00:00Z','1234','Payment processing fees','','unmatched');

COMMIT;
" 
```

### Test with a sample transaction

Start a workflow by sending a transaction event. For quick testing, reuse one of the seeded IDs (e.g. `txn_001`) and `tenant_1`.

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_001",
    "event_type": "transaction.created",
    "event_time": "2026-05-11T00:00:00Z",
    "transaction": {
      "transaction_id": "txn_001",
      "tenant_id": "tenant_1",
      "source": "reap_card",
      "status": "settled",
      "merchant_name": "Amazon Web Services",
      "merchant_normalized_name": "amazon web services",
      "mcc": "7372",
      "mcc_description": "Computer programming / data processing",
      "amount": 12.34,
      "currency": "USD",
      "billing_amount": 12.34,
      "billing_currency": "USD",
      "fx_rate": 1.0,
      "transaction_date": "2026-05-01",
      "posted_at": "2026-05-01T10:00:00Z",
      "card_last4": "1234",
      "description": "AWS monthly subscription",
      "receipt_id": "",
      "receipt_status": "unmatched"
    }
  }'
```

### Poll the review queue

```bash
curl http://localhost:8080/queue/tenant_acme_sg
```

### Submit an accountant decision

```bash
curl -X POST http://localhost:8080/review \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_card_001",
    "tenant_id": "tenant_acme_sg",
    "action": "ACCEPTED",
    "accountant_id": "accountant_sarah"
  }'
```

## What is NOT implemented (explicitly out of scope)

- Accounting platform sync (Xero/QBO/NetSuite) — noted in HLD as out of scope
- Append-only audit log export — noted in HLD as out of scope  
- Real embedding API call — `embedText()` in store/postgres.go returns a zero vector; replace with OpenAI text-embedding-3-small or equivalent
- Kafka consumer — `POST /ingest` is the stand-in; production wires a Sarama consumer that calls `client.ExecuteWorkflow`
- Receipt OCR pipeline — `FetchReceipt` queries the `receipt_ocr` table; OCR ingestion is a separate service

## Answers to your clarifying questions

**1. Confidence threshold calibration**: stored in `confidence_thresholds` table, per-tenant or global. The workflow fetches them at runtime — change them in Postgres and the next workflow execution picks them up, no redeploy. The cost model for tuning: `override_rate` from correction_events (override_count / total) is the signal. If override_rate for the 0.85-0.90 band is low, raise auto_approve_min. If it's high, lower it.

**2. CoA structure**: flat `{ code: label }` as agreed. Stored in `coa_schema` table. The tagging prompt lists all codes explicitly as the closed output space.

**3. Cold start**: `EnrichTransaction` returns empty RAGNeighbours on a cold tenant. The prompt explicitly tells the LLM "No past transactions available — reason from CoA descriptions." Confidence will be lower (~0.5-0.65), routing more to IN_REVIEW initially.

**4. Unreviewed transactions**: the workflow waits up to 7 days (configurable). On deadline, it fails-open: auto-confirms at the suggestion with a "deadline_auto_confirm" source recorded in correction_events.

**5. Accounting sync**: one-way for now. The `tagging` table is the source of truth. Edits in Xero do not flow back — this is a known gap in the feedback loop noted in the code.
