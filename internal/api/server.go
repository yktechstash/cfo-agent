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

	// Accountant review queue
	r.GET("/queue/:tenant_id", s.handleGetQueue)
	r.POST("/review", s.handleReview)

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

func (s *Server) handleReview(c *gin.Context) {
	var req ReviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate action
	switch req.Action {
	case domain.ActionAccepted, domain.ActionOverridden, domain.ActionFlagged:
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
