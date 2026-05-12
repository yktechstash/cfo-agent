package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"

	"github.com/reap/cfo-agent/internal/activity"
	"github.com/reap/cfo-agent/internal/domain"
	"github.com/reap/cfo-agent/internal/store"
	"github.com/reap/cfo-agent/internal/workflow"
)

type Server struct {
	router         *gin.Engine
	temporalClient client.Client
}

func New(temporalClient client.Client) *Server {
	s := &Server{temporalClient: temporalClient}
	r := gin.Default()

	// Ingestion endpoint — in prod this is triggered by Kafka consumer,
	// but exposed as HTTP for testing and manual re-runs.
	r.POST("/ingest", s.handleIngest)

	// Accountant worklist + review actions
	r.GET("/worklist/:tenant_id", s.handleGetWorklist)
	r.POST("/review", s.handleReview)
	r.POST("/escalated/review", s.handleEscalatedReview)
	r.PUT("/confidence-thresholds/:tenant_id", s.handleUpdateThresholds)

	r.GET("/ui/:tenant_id", s.handleUI)

	// Transaction status polling
	r.GET("/status/:txn_id", s.handleGetStatus)

	// Nightly learning loop trigger (called by Cloud Scheduler)
	r.POST("/learning-loop/:tenant_id", s.handleLearningLoop)

	s.router = r
	return s
}

func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}

// ── POST /ingest ──────────────────────────────────────────────────────────
// Starts a Temporal workflow for each transaction event.
// In production the Kafka consumer calls StartWorkflow directly —
// this endpoint exists for manual triggers and integration tests.

func (s *Server) handleIngest(c *gin.Context) {
	var event domain.TransactionEvent
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if event.Txn.TransactionID == "" || event.Txn.TenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "transaction_id and tenant_id are required"})
		return
	}
	if err := store.UpsertTransaction(c.Request.Context(), event.Txn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	opts := client.StartWorkflowOptions{
		// WorkflowID is deterministic on txn_id — prevents duplicate processing
		// if the Kafka consumer retries on the same event.
		ID:        "tagging-" + event.Txn.TransactionID,
		TaskQueue: workflow.TaskQueueTagging,
	}

	we, err := s.temporalClient.ExecuteWorkflow(
		c.Request.Context(),
		opts,
		workflow.TaggingWorkflow,
		workflow.TaggingWorkflowInput{Event: event},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"workflow_id": we.GetID(),
		"run_id":      we.GetRunID(),
		"txn_id":      event.Txn.TransactionID,
		"tenant_id":   event.Txn.TenantID,
		"message":     "transaction persisted and workflow started",
	})
}

// ── GET /queue/:tenant_id ─────────────────────────────────────────────────
// Returns all pending review queue items for the accountant UI.

func (s *Server) handleGetQueue(c *gin.Context) {
	tenantID := c.Param("tenant_id")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id required"})
		return
	}

	items, err := store.FetchReviewQueue(c.Request.Context(), tenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Attach CoA options for the override dropdown
	coaEntries, err := store.FetchCoAEntries(c.Request.Context(), tenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	for i := range items {
		items[i].CoAOptions = coaEntries
	}

	c.JSON(http.StatusOK, gin.H{
		"tenant_id": tenantID,
		"count":     len(items),
		"items":     items,
	})
}

// ── POST /review ──────────────────────────────────────────────────────────
// Accountant submits a decision. Sends a Temporal signal to the waiting workflow.

type ReviewRequest struct {
	TransactionID string                  `json:"transaction_id" binding:"required"`
	TenantID      string                  `json:"tenant_id"      binding:"required"`
	Action        domain.CorrectionAction `json:"action"         binding:"required"`
	FinalCode     string                  `json:"final_code"`
	AccountantID  string                  `json:"accountant_id"  binding:"required"`
}

type EscalatedReviewRequest struct {
	TransactionID string `json:"transaction_id" binding:"required"`
	TenantID      string `json:"tenant_id"      binding:"required"`
	FinalCode     string `json:"final_code"     binding:"required"`
	AccountantID  string `json:"accountant_id"  binding:"required"`
}

type UpdateThresholdsRequest struct {
	AutoApproveMin float64 `json:"auto_approve_min" binding:"required"`
	ReviewMin      float64 `json:"review_min"       binding:"required"`
}

func (s *Server) handleReview(c *gin.Context) {
	var req ReviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate action
	switch req.Action {
	case domain.ActionAccepted, domain.ActionOverridden, domain.ActionDenied, domain.ActionFlagged:
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action"})
		return
	}

	// OVERRIDDEN requires a final_code
	if req.Action == domain.ActionOverridden && req.FinalCode == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "final_code required for OVERRIDDEN action"})
		return
	}

	// Send signal to the waiting Temporal workflow
	sig := workflow.ReviewSignal{
		Action:       req.Action,
		FinalCode:    req.FinalCode,
		AccountantID: req.AccountantID,
	}

	workflowID := "tagging-" + req.TransactionID
	err := s.temporalClient.SignalWorkflow(
		c.Request.Context(),
		workflowID,
		"", // latest run
		workflow.SignalReviewAction,
		sig,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"transaction_id": req.TransactionID,
		"action":         string(req.Action),
		"accepted_at":    time.Now().UTC(),
	})
}

func (s *Server) handleEscalatedReview(c *gin.Context) {
	var req EscalatedReviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	sig := workflow.EscalationSignal{
		FinalCode:    req.FinalCode,
		AccountantID: req.AccountantID,
	}

	workflowID := "tagging-" + req.TransactionID
	err := s.temporalClient.SignalWorkflow(
		c.Request.Context(),
		workflowID,
		"",
		workflow.SignalEscalationAction,
		sig,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"transaction_id": req.TransactionID,
		"action":         "ESCALATION_RESOLVED",
		"accepted_at":    time.Now().UTC(),
	})
}

func (s *Server) handleGetWorklist(c *gin.Context) {
	tenantID := c.Param("tenant_id")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id required"})
		return
	}
	items, err := store.FetchWorklist(c.Request.Context(), tenantID, 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	coaEntries, err := store.FetchCoAEntries(c.Request.Context(), tenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tenant_id": tenantID, "count": len(items), "items": items, "coa_entries": coaEntries})
}

func (s *Server) handleUpdateThresholds(c *gin.Context) {
	tenantID := c.Param("tenant_id")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id required"})
		return
	}

	var req UpdateThresholdsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.AutoApproveMin < 0 || req.AutoApproveMin > 1 || req.ReviewMin < 0 || req.ReviewMin > 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "thresholds must be between 0 and 1"})
		return
	}
	if req.AutoApproveMin < req.ReviewMin {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auto_approve_min must be >= review_min"})
		return
	}
	if err := store.UpsertThresholds(c.Request.Context(), tenantID, req.AutoApproveMin, req.ReviewMin); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"tenant_id":        tenantID,
		"auto_approve_min": req.AutoApproveMin,
		"review_min":       req.ReviewMin,
		"updated_at":       time.Now().UTC(),
	})
}

func (s *Server) handleUI(c *gin.Context) {
	html := `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>reap-cfo-agent</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;margin:24px;background:#fafafa;}
    h1{margin:0 0 12px 0;}
    .row{display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start;}
    .card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:12px;min-width:320px;flex:1;box-shadow:0 1px 2px rgba(0,0,0,.04);}
    table{width:100%;border-collapse:collapse;font-size:13px;}
    th,td{padding:6px 8px;border-bottom:1px solid #f1f5f9;text-align:left;vertical-align:top;}
    .muted{color:#64748b;font-size:12px;}
    .error{margin:12px 0;padding:10px 12px;border:1px solid #fecaca;background:#fef2f2;color:#991b1b;border-radius:8px;display:none;}
    .empty{color:#94a3b8;font-style:italic;}
    input{padding:6px 8px;margin-right:6px;max-width:110px;}
    button{padding:6px 10px;cursor:pointer;}
  </style>
</head>
<body>
  <h1>Tenant: <span id="tenant"></span></h1>
  <div class="muted">Completed shows both auto-tagged items and transactions moved from manual review. In review = waiting for accountant. Escalated = exception flow.</div>
  <div id="error" class="error"></div>
  <div class="row" style="margin-top:16px">
    <div class="card"><h3>Completed</h3><table id="auto"></table></div>
    <div class="card"><h3>In review</h3><table id="review"></table></div>
    <div class="card"><h3>Escalated</h3><table id="esc"></table></div>
  </div>
<script>
var tenantId = window.location.pathname.split('/').pop();
var base = '';
var coaEntries = [];
document.getElementById('tenant').textContent = tenantId;

function showError(msg) {
  var el = document.getElementById('error');
  el.style.display = msg ? 'block' : 'none';
  el.textContent = msg || '';
}

function esc(v) {
  if (v === null || v === undefined) return '';
  return String(v)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function table(el, rows, opts) {
  var headers = opts.headers;
  var head = '<tr>' + headers.map(function(h){ return '<th>' + h + '</th>'; }).join('') + '</tr>';
  if (!rows || !rows.length) {
    el.innerHTML = head + '<tr><td class="empty" colspan="' + headers.length + '">No items</td></tr>';
    return;
  }
  var body = rows.map(opts.row).join('');
  el.innerHTML = head + body;
}

function fetchJSON(url) {
  return fetch(url).then(function(res) {
    return res.json().then(function(data) {
      if (!res.ok) throw new Error(data.error || res.statusText || 'Request failed');
      return data;
    });
  });
}

function renderAuto(items) {
  table(document.getElementById('auto'), items, {
    headers: ['txn', 'merchant', 'amount', 'date', 'tag'],
    row: function(r) {
      return '<tr><td>' + esc(r.transaction_id) + '</td><td>' + esc(r.merchant_name) + '</td><td>' + esc(r.currency) + ' ' + esc(r.amount) + '</td><td>' + esc(r.transaction_date) + '</td><td>' + esc(r.tag || '') + '</td></tr>';
    }
  });
}

function renderReview(items) {
  table(document.getElementById('review'), items, {
    headers: ['txn', 'merchant', 'suggested', 'confidence', 'actions'],
    row: function(r) {
      return '<tr>' +
        '<td>' + esc(r.transaction_id) + '</td>' +
        '<td>' + esc(r.merchant_name) + '</td>' +
        '<td>' + esc(r.suggested_code || '') + '</td>' +
        '<td>' + esc(r.confidence || '') + '</td>' +
        '<td>' +
          '<button type="button" onclick="approveReview(\'' + esc(r.transaction_id) + '\')">Approve</button>' +
          '<button type="button" onclick="denyReview(\'' + esc(r.transaction_id) + '\')">Deny</button>' +
        '</td>' +
        '</tr>';
    }
  });
}

function coaSelectHtml() {
  if (!coaEntries.length) {
    return '<option value="">No CoA</option>';
  }
  return coaEntries.map(function(entry) {
    return '<option value="' + esc(entry.code) + '">' + esc(entry.code + ' - ' + entry.label) + '</option>';
  }).join('');
}

function renderEscalated(items) {
  table(document.getElementById('esc'), items, {
    headers: ['txn', 'merchant', 'amount', 'resolve'],
    row: function(r) {
      return '<tr>' +
        '<td>' + esc(r.transaction_id) + '</td>' +
        '<td>' + esc(r.merchant_name) + '</td>' +
        '<td>' + esc(r.currency) + ' ' + esc(r.amount) + '</td>' +
        '<td><form onsubmit="return resolveEsc(event, \'' + esc(r.transaction_id) + '\')">' +
        '<select name="final_code" required>' + coaSelectHtml() + '</select>' +
        '<input name="accountant_id" placeholder="accountant" required />' +
        '<button type="submit">Resolve</button>' +
        '</form></td>' +
        '</tr>';
    }
  });
}

function load() {
  showError('');
  return fetchJSON(base + '/worklist/' + tenantId).then(function(data) {
    var items = data.items || [];
    coaEntries = data.coa_entries || [];
    var autoItems = [];
    var reviewItems = [];
    var escalatedItems = [];

    items.forEach(function(item) {
      if (item.route === 'AUTO_TAGGED' || item.route === 'MANUAL_REVIEW') autoItems.push(item);
      else if (item.route === 'IN_REVIEW') reviewItems.push(item);
      else if (item.route === 'ESCALATED') escalatedItems.push(item);
    });

    renderAuto(autoItems);
    renderReview(reviewItems);
    renderEscalated(escalatedItems);
  }).catch(function(err) {
    renderAuto([]);
    renderReview([]);
    renderEscalated([]);
    showError(err.message || 'Failed to load UI');
  });
}

function waitForReviewMove(txnId, attempts) {
  if (!attempts) return load();
  return new Promise(function(resolve) {
    setTimeout(resolve, 700);
  }).then(function() {
    return fetchJSON(base + '/worklist/' + tenantId);
  }).then(function(data) {
    var items = data.items || [];
    var stillInReview = items.some(function(item) {
      return item.transaction_id === txnId && item.route === 'IN_REVIEW';
    });
    if (!stillInReview) {
      return load();
    }
    return waitForReviewMove(txnId, attempts - 1);
  });
}

function sendReviewAction(payload) {
  return fetch(base + '/review', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).then(function(res) {
    return res.json().then(function(data) {
      if (!res.ok) throw new Error(data.error || res.statusText || 'Review failed');
      return data;
    });
  }).then(function() {
    return waitForReviewMove(payload.transaction_id, 8);
  }).catch(function(err) {
    showError(err.message || 'Failed to submit review action');
    return false;
  });
}

function approveReview(txnId) {
  var accountantId = window.prompt('Accountant ID for approval?', 'acct_123');
  if (!accountantId) return false;
  return sendReviewAction({
    transaction_id: txnId,
    tenant_id: tenantId,
    action: 'ACCEPTED',
    accountant_id: accountantId
  });
}

function denyReview(txnId) {
  var accountantId = window.prompt('Accountant ID for denial?', 'acct_123');
  if (!accountantId) return false;
  return sendReviewAction({
    transaction_id: txnId,
    tenant_id: tenantId,
    action: 'DENIED',
    accountant_id: accountantId
  });
}

function waitForEscalationMove(txnId, attempts) {
  if (!attempts) return load();
  return new Promise(function(resolve) {
    setTimeout(resolve, 700);
  }).then(function() {
    return fetchJSON(base + '/worklist/' + tenantId);
  }).then(function(data) {
    var items = data.items || [];
    var stillEscalated = items.some(function(item) {
      return item.transaction_id === txnId && item.route === 'ESCALATED';
    });
    if (!stillEscalated) {
      return load();
    }
    return waitForEscalationMove(txnId, attempts - 1);
  });
}

function resolveEsc(e, txnId) {
  e.preventDefault();
  var fd = new FormData(e.target);
  var payload = {
    transaction_id: txnId,
    tenant_id: tenantId,
    final_code: fd.get('final_code'),
    accountant_id: fd.get('accountant_id')
  };
  return fetch(base + '/escalated/review', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).then(function(res) {
    return res.json().then(function(data) {
      if (!res.ok) throw new Error(data.error || res.statusText || 'Resolve failed');
      return data;
    });
  }).then(function() {
    return waitForEscalationMove(txnId, 8);
  }).catch(function(err) {
    showError(err.message || 'Failed to resolve escalated item');
    return false;
  });
}

load();
</script>
</body>
</html>`
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
}

// ── GET /status/:txn_id ───────────────────────────────────────────────────

func (s *Server) handleGetStatus(c *gin.Context) {
	txnID := c.Param("txn_id")

	// Query Postgres — cheaper than querying Temporal for simple status polls
	var status string
	var updatedAt time.Time
	err := func() error {
		// Direct query without exposing pool — use store layer in production
		return nil
	}()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"txn_id":     txnID,
		"status":     status,
		"updated_at": updatedAt,
	})
}

// ── POST /learning-loop/:tenant_id ────────────────────────────────────────
// Triggered by Cloud Scheduler at 02:00 per tenant timezone.

func (s *Server) handleLearningLoop(c *gin.Context) {
	tenantID := c.Param("tenant_id")

	go func() {
		ctx := c.Request.Context()
		if err := activity.RunLearningLoop(ctx, tenantID); err != nil {
			// Log — the cron will retry on next run
			_ = err
		}
	}()

	c.JSON(http.StatusAccepted, gin.H{
		"tenant_id": tenantID,
		"message":   "learning loop started",
	})
}
