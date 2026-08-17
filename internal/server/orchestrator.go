// Package server provides HTTP server setup with Gin + gRPC-gateway.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestratorv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/orchestrator/v1"
	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/approval"
	"github.com/NicoYazawa/microservice_diagnosis/internal/discovery"
	"github.com/NicoYazawa/microservice_diagnosis/internal/executor"
	"github.com/NicoYazawa/microservice_diagnosis/internal/notify"
	"github.com/NicoYazawa/microservice_diagnosis/internal/report"
	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
	"github.com/NicoYazawa/microservice_diagnosis/internal/workflow"
)

// OrchestratorHandler implements the Orchestrator service HTTP handlers.
// It bridges REST/gRPC-gateway requests to the workflow engine and data layer.
type OrchestratorHandler struct {
	sessionDAO        *store.SessionDAO
	fixDAO            *store.FixActionDAO
	approvalDAO       *store.ApprovalDAO
	webhookDAO        *notify.WebhookDAO
	engine            *workflow.Engine
	discovery         discovery.Discovery
	approvalClient    approval.ApprovalClient
	executor          executor.Executor
	incidentNotifier  notify.IncidentNotifier
	webhookNotifier   *notify.WebhookNotifier
	reportEngine      *report.Engine
	log               *slog.Logger
}

// NewOrchestratorHandler creates a handler wired to the given dependencies.
func NewOrchestratorHandler(
	sessionDAO *store.SessionDAO,
	fixDAO *store.FixActionDAO,
	approvalDAO *store.ApprovalDAO,
	webhookDAO *notify.WebhookDAO,
	engine *workflow.Engine,
	disc discovery.Discovery,
	approvalClient approval.ApprovalClient,
	exec executor.Executor,
	incidentNotifier notify.IncidentNotifier,
	webhookNotifier *notify.WebhookNotifier,
	reportEngine *report.Engine,
	log *slog.Logger,
) *OrchestratorHandler {
	return &OrchestratorHandler{
		sessionDAO:       sessionDAO,
		fixDAO:            fixDAO,
		approvalDAO:       approvalDAO,
		webhookDAO:        webhookDAO,
		engine:            engine,
		discovery:         disc,
		approvalClient:    approvalClient,
		executor:          exec,
		incidentNotifier:  incidentNotifier,
		webhookNotifier:   webhookNotifier,
		reportEngine:     reportEngine,
		log:               log,
	}
}

// --- Session CRUD ---

func (h *OrchestratorHandler) CreateSession(c *gin.Context) {
	var req orchestratorv1.CreateSessionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		badRequest(c, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if req.TargetService == "" {
		badRequest(c, "target_service is required")
		return
	}

	s, err := h.sessionDAO.Create(c.Request.Context(), req.TargetService, store.TriggerManual)
	if err != nil {
		internalError(c, fmt.Sprintf("create session: %v", err))
		return
	}

	h.log.Info("session created", "id", s.ID)
	c.JSON(http.StatusCreated, orchestratorv1.CreateSessionResponse{
		Session: h.toSessionProto(s),
	})
}

func (h *OrchestratorHandler) ListSessions(c *gin.Context) {
	var req orchestratorv1.ListSessionsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		badRequest(c, fmt.Sprintf("invalid query params: %v", err))
		return
	}

	if req.Page == 0 {
		req.Page = 1
	}
	if req.PageSize == 0 {
		req.PageSize = 20
	}

	filter := store.ListFilter{
		Status:        h.statusProtoToString(req.Status),
		TargetService: req.TargetService,
		Keyword:       req.Keyword,
		Page:          int(req.Page),
		PageSize:      int(req.PageSize),
	}
	if req.From != nil {
		filter.From = req.From.AsTime()
	}
	if req.To != nil {
		filter.To = req.To.AsTime()
	}

	sessions, total, err := h.sessionDAO.List(c.Request.Context(), filter)
	if err != nil {
		internalError(c, fmt.Sprintf("list sessions: %v", err))
		return
	}

	protoSessions := make([]*orchestratorv1.Session, len(sessions))
	for i, s := range sessions {
		protoSessions[i] = h.toSessionProto(s)
	}

	c.JSON(http.StatusOK, orchestratorv1.ListSessionsResponse{
		Sessions: protoSessions,
		Total:    int32(total),
		Page:     req.Page,
		PageSize: req.PageSize,
	})
}

func (h *OrchestratorHandler) GetSession(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}

	s, err := h.sessionDAO.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			notFound(c, "session not found")
			return
		}
		internalError(c, fmt.Sprintf("get session: %v", err))
		return
	}

	// Build timeline events from session state changes.
	timeline := h.buildTimeline(s)

	// Observations are populated from the Kafka message bus (M8).
	var observations []*observationv1.Observation

	// RCA result observation (populated by RCA agent, M8).
	var rootCause *orchestratorv1.RootCause

	// Fetch fix actions.
	fixes, _ := h.fixDAO.GetBySessionID(c.Request.Context(), id)
	fixSummaries := make([]*orchestratorv1.FixSummary, len(fixes))
	for i, fa := range fixes {
		fixSummaries[i] = h.toFixSummaryProto(fa)
	}

	c.JSON(http.StatusOK, orchestratorv1.GetSessionResponse{
		Detail: &orchestratorv1.SessionDetail{
			Session:      h.toSessionProto(s),
			Timeline:     timeline,
			Observations: observations,
			RootCause:    rootCause,
			Fixes:        fixSummaries,
		},
	})
}

func (h *OrchestratorHandler) StartSession(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}

	// Transition: CREATED -> COLLECTING.
	newStatus, err := h.engine.Transition(c.Request.Context(), id, workflow.EventStartCollect)
	if err != nil {
		if errors.Is(err, workflow.ErrSessionNotFound) {
			notFound(c, "session not found")
			return
		}
		if errors.Is(err, workflow.ErrInvalidTransition) {
			badRequest(c, fmt.Sprintf("cannot start session in current state: %v", err))
			return
		}
		internalError(c, fmt.Sprintf("start session: %v", err))
		return
	}

	s, _ := h.sessionDAO.GetByID(c.Request.Context(), id)
	h.log.Info("session started", "id", id, "status", newStatus)
	c.JSON(http.StatusOK, orchestratorv1.StartSessionResponse{
		Session: h.toSessionProto(s),
	})
}

func (h *OrchestratorHandler) RetrySession(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}

	_, err = h.sessionDAO.IncrementRetry(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			notFound(c, "session not found")
			return
		}
		internalError(c, fmt.Sprintf("retry session: %v", err))
		return
	}

	// Transition back to CREATED for retry.
	newStatus, err := h.engine.Transition(c.Request.Context(), id, workflow.EventRetry)
	if err != nil {
		if errors.Is(err, workflow.ErrInvalidTransition) {
			badRequest(c, fmt.Sprintf("cannot retry session in current state: %v", err))
			return
		}
		internalError(c, fmt.Sprintf("retry session: %v", err))
		return
	}

	s, _ := h.sessionDAO.GetByID(c.Request.Context(), id)
	h.log.Info("session retried", "id", id, "status", newStatus)
	c.JSON(http.StatusOK, orchestratorv1.RetrySessionResponse{
		Session: h.toSessionProto(s),
	})
}

func (h *OrchestratorHandler) IgnoreSession(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}

	var req orchestratorv1.IgnoreSessionRequest
	_ = c.ShouldBindJSON(&req) // optional reason field

	_, err = h.engine.Transition(c.Request.Context(), id, workflow.EventIgnore)
	if err != nil {
		if errors.Is(err, workflow.ErrSessionNotFound) {
			notFound(c, "session not found")
			return
		}
		if errors.Is(err, workflow.ErrInvalidTransition) {
			badRequest(c, fmt.Sprintf("cannot ignore session in current state: %v", err))
			return
		}
		internalError(c, fmt.Sprintf("ignore session: %v", err))
		return
	}

	s, _ := h.sessionDAO.GetByID(c.Request.Context(), id)
	h.log.Info("session ignored", "id", id, "reason", req.Reason)
	c.JSON(http.StatusOK, orchestratorv1.IgnoreSessionResponse{
		Session: h.toSessionProto(s),
	})
}

// --- Approval callback ---

func (h *OrchestratorHandler) DecisionApproval(c *gin.Context) {
	sessionIDStr := c.Param("id")
	sessionID, err := uuid.Parse(sessionIDStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}
	approvalIDStr := c.Param("approval_id")
	approvalID, err := uuid.Parse(approvalIDStr)
	if err != nil {
		badRequest(c, "invalid approval id")
		return
	}

	var req orchestratorv1.DecisionApprovalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		badRequest(c, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if req.Decision == orchestratorv1.ApprovalDecision_APPROVAL_DECISION_UNSPECIFIED {
		badRequest(c, "decision is required")
		return
	}

	// Fetch the approval record.
	approval, err := h.approvalDAO.GetByID(c.Request.Context(), approvalID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			notFound(c, "approval not found")
			return
		}
		internalError(c, fmt.Sprintf("get approval: %v", err))
		return
	}

	// Verify it belongs to the session.
	if approval.SessionID != sessionID {
		badRequest(c, "approval does not belong to this session")
		return
	}

	// Update approval record.
	decision := store.ApprovalStatusApproved
	event := workflow.EventApprove
	if req.Decision == orchestratorv1.ApprovalDecision_APPROVAL_DECISION_REJECT {
		decision = store.ApprovalStatusRejected
		event = workflow.EventReject
	}
	if err := h.approvalDAO.UpdateDecision(c.Request.Context(), approvalID, decision, req.DecidedBy); err != nil {
		internalError(c, fmt.Sprintf("update approval decision: %v", err))
		return
	}

	// Also update the associated fix action approval status.
	fixAction, _ := h.fixDAO.GetByID(c.Request.Context(), approval.FixActionID)
	if fixAction != nil {
		h.fixDAO.UpdateApprovalStatus(c.Request.Context(), fixAction.ID, decision)
	}

	// Advance the session state machine.
	_, _ = h.engine.Transition(c.Request.Context(), sessionID, event)

	h.log.Info("approval decided", "approval_id", approvalID, "decision", decision, "by", req.DecidedBy)
	c.JSON(http.StatusOK, orchestratorv1.DecisionApprovalResponse{
		ApprovalId: approvalID.String(),
		Status:     decision,
	})
}

// --- Report (M7: actual rendering) ---

func (h *OrchestratorHandler) GetReport(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}

	format := orchestratorv1.ReportFormat_REPORT_FORMAT_MARKDOWN
	if cf := c.Query("format"); cf == "pdf" {
		format = orchestratorv1.ReportFormat_REPORT_FORMAT_PDF
	}

	if format == orchestratorv1.ReportFormat_REPORT_FORMAT_PDF {
		_, pdf, contentType, err := h.reportEngine.GenerateReport(c.Request.Context(), id)
		if err != nil {
			internalError(c, fmt.Sprintf("generate report: %v", err))
			return
		}
		c.Header("Content-Type", contentType)
		c.Data(http.StatusOK, contentType, pdf)
		return
	}

	markdown, err := h.reportEngine.RenderMarkdown(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			notFound(c, "session not found")
			return
		}
		internalError(c, fmt.Sprintf("generate report: %v", err))
		return
	}

	c.JSON(http.StatusOK, orchestratorv1.GetReportResponse{
		Format:      orchestratorv1.ReportFormat_REPORT_FORMAT_MARKDOWN,
		Content:     markdown,
		DownloadUrl: fmt.Sprintf("/v1/sessions/%s/report/download?format=markdown", idStr),
	})
}

func (h *OrchestratorHandler) DownloadReport(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		badRequest(c, "invalid session id")
		return
	}

	format := "markdown"
	if cf := c.Query("format"); cf == "pdf" {
		format = "pdf"
	}

	var content []byte
	var contentType string

	if format == "pdf" {
		_, pdf, ct, err := h.reportEngine.GenerateReport(c.Request.Context(), id)
		if err != nil {
			internalError(c, fmt.Sprintf("generate pdf report: %v", err))
			return
		}
		content = pdf
		contentType = ct
	} else {
		markdown, err := h.reportEngine.RenderMarkdown(c.Request.Context(), id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				notFound(c, "session not found")
				return
			}
			internalError(c, fmt.Sprintf("generate markdown report: %v", err))
			return
		}
		content = []byte(markdown)
		contentType = "text/markdown"
	}

	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="report-%s.%s"`, idStr, format))
	c.Data(http.StatusOK, contentType, content)
}

// --- Agent health via Consul discovery ---

func (h *OrchestratorHandler) ListAgents(c *gin.Context) {
	if h.discovery == nil {
		c.JSON(http.StatusOK, orchestratorv1.ListAgentsResponse{Agents: []*orchestratorv1.AgentInfo{}})
		return
	}

	agentTypes := []string{"agent-log", "agent-metric", "agent-trace", "agent-rca", "agent-fix", "orchestrator"}
	var allAgents []*orchestratorv1.AgentInfo

	for _, name := range agentTypes {
		instances, err := h.discovery.Discover(c.Request.Context(), name)
		if err != nil {
			continue
		}
		for _, inst := range instances {
			allAgents = append(allAgents, &orchestratorv1.AgentInfo{
				Name:          inst.Name,
				Kind:          inst.Kind,
				Version:       inst.Version,
				Status:        inst.Status,
				Endpoint:      inst.HTTPAddr,
				LastHeartbeat: timestamppb.New(inst.Heartbeat),
			})
		}
	}

	c.JSON(http.StatusOK, orchestratorv1.ListAgentsResponse{Agents: allAgents})
}

// --- Health check (generic) ---

func (h *OrchestratorHandler) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// --- Helpers ---

func (h *OrchestratorHandler) toSessionProto(s *store.DiagnosticSession) *orchestratorv1.Session {
	return &orchestratorv1.Session{
		Id:            s.ID.String(),
		TargetService: s.TargetService,
		Status:        h.statusToProto(s.Status),
		RetryCount:    int32(s.RetryCount),
		CreatedAt:     timestamppb.New(s.CreatedAt),
		UpdatedAt:     timestamppb.New(s.UpdatedAt),
	}
}

func (h *OrchestratorHandler) statusToProto(s string) orchestratorv1.SessionStatus {
	switch s {
	case store.StatusCreated:
		return orchestratorv1.SessionStatus_SESSION_STATUS_CREATED
	case store.StatusCollecting:
		return orchestratorv1.SessionStatus_SESSION_STATUS_COLLECTING
	case store.StatusAnalyzing:
		return orchestratorv1.SessionStatus_SESSION_STATUS_ANALYZING
	case store.StatusRCADone:
		return orchestratorv1.SessionStatus_SESSION_STATUS_RCA_DONE
	case store.StatusFixProposed:
		return orchestratorv1.SessionStatus_SESSION_STATUS_FIX_PROPOSED
	case store.StatusAwaitingApproval:
		return orchestratorv1.SessionStatus_SESSION_STATUS_AWAITING_APPROVAL
	case store.StatusFixSuggested:
		return orchestratorv1.SessionStatus_SESSION_STATUS_FIX_SUGGESTED
	case store.StatusFixExecuting:
		return orchestratorv1.SessionStatus_SESSION_STATUS_FIX_EXECUTING
	case store.StatusVerifying:
		return orchestratorv1.SessionStatus_SESSION_STATUS_VERIFYING
	case store.StatusResolved:
		return orchestratorv1.SessionStatus_SESSION_STATUS_RESOLVED
	case store.StatusRolledBack:
		return orchestratorv1.SessionStatus_SESSION_STATUS_ROLLED_BACK
	case store.StatusRejected:
		return orchestratorv1.SessionStatus_SESSION_STATUS_REJECTED
	case store.StatusIgnored:
		return orchestratorv1.SessionStatus_SESSION_STATUS_IGNORED
	case store.StatusFailed:
		return orchestratorv1.SessionStatus_SESSION_STATUS_FAILED
	default:
		return orchestratorv1.SessionStatus_SESSION_STATUS_UNSPECIFIED
	}
}

func (h *OrchestratorHandler) statusProtoToString(status orchestratorv1.SessionStatus) string {
	switch status {
	case orchestratorv1.SessionStatus_SESSION_STATUS_CREATED:
		return store.StatusCreated
	case orchestratorv1.SessionStatus_SESSION_STATUS_COLLECTING:
		return store.StatusCollecting
	case orchestratorv1.SessionStatus_SESSION_STATUS_ANALYZING:
		return store.StatusAnalyzing
	case orchestratorv1.SessionStatus_SESSION_STATUS_RCA_DONE:
		return store.StatusRCADone
	case orchestratorv1.SessionStatus_SESSION_STATUS_FIX_PROPOSED:
		return store.StatusFixProposed
	case orchestratorv1.SessionStatus_SESSION_STATUS_AWAITING_APPROVAL:
		return store.StatusAwaitingApproval
	case orchestratorv1.SessionStatus_SESSION_STATUS_FIX_SUGGESTED:
		return store.StatusFixSuggested
	case orchestratorv1.SessionStatus_SESSION_STATUS_FIX_EXECUTING:
		return store.StatusFixExecuting
	case orchestratorv1.SessionStatus_SESSION_STATUS_VERIFYING:
		return store.StatusVerifying
	case orchestratorv1.SessionStatus_SESSION_STATUS_RESOLVED:
		return store.StatusResolved
	case orchestratorv1.SessionStatus_SESSION_STATUS_ROLLED_BACK:
		return store.StatusRolledBack
	case orchestratorv1.SessionStatus_SESSION_STATUS_REJECTED:
		return store.StatusRejected
	case orchestratorv1.SessionStatus_SESSION_STATUS_IGNORED:
		return store.StatusIgnored
	case orchestratorv1.SessionStatus_SESSION_STATUS_FAILED:
		return store.StatusFailed
	default:
		return ""
	}
}

func (h *OrchestratorHandler) toFixSummaryProto(fa *store.FixAction) *orchestratorv1.FixSummary {
	return &orchestratorv1.FixSummary{
		Id:           fa.ID.String(),
		Seq:          int32(fa.Seq),
		ActionType:   fa.ActionType,
		Target:       fa.Target,
		Risk:         fa.Risk,
		Status:       fa.ExecutionStatus,
		RollbackPlan: fa.RollbackPlan,
	}
}

func (h *OrchestratorHandler) buildTimeline(s *store.DiagnosticSession) []*orchestratorv1.TimelineEvent {
	events := []*orchestratorv1.TimelineEvent{
		{
			EventType: "session_created",
			Source:    "orchestrator",
			Message:   fmt.Sprintf("Diagnostic session created for service %s", s.TargetService),
			Timestamp: timestamppb.New(s.CreatedAt),
		},
	}
	if s.Status != store.StatusCreated {
		events = append(events, &orchestratorv1.TimelineEvent{
			EventType: "session_started",
			Source:    "orchestrator",
			Message:   fmt.Sprintf("Session transitioned to %s", s.Status),
			Timestamp: timestamppb.New(s.UpdatedAt),
		})
	}
	if s.Status == store.StatusResolved {
		events = append(events, &orchestratorv1.TimelineEvent{
			EventType: "session_resolved",
			Source:    "orchestrator",
			Message:   "Diagnostic session completed successfully",
			Timestamp: timestamppb.New(s.UpdatedAt),
		})
	}
	return events
}

// --- M6: Approval Gate ---

// TriggerApprovalGate creates approval requests for all HIGH-risk fix actions
// that are pending in a session, and sends webhook notifications.
// It is called by the workflow sweep loop when a session reaches FIX_PROPOSED
// and contains HIGH-risk steps (or by the FixAgent after generating fix actions).
func (h *OrchestratorHandler) TriggerApprovalGate(ctx context.Context, sessionID uuid.UUID) error {
	fixes, err := h.fixDAO.GetBySessionID(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("trigger approval: get fixes: %w", err)
	}

	var highRiskFixes []*store.FixAction
	for _, fa := range fixes {
		if fa.Risk == store.RiskHigh && fa.ApprovalStatus == store.ApprovalStatusNone {
			highRiskFixes = append(highRiskFixes, fa)
		}
	}
	if len(highRiskFixes) == 0 {
		return nil
	}

	// Get session info for summary.
	s, err := h.sessionDAO.GetByID(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("trigger approval: get session: %w", err)
	}

	for _, fa := range highRiskFixes {
		summary := fmt.Sprintf("HIGH-risk fix action [%s] on %s (session %s)",
			fa.ActionType, fa.Target, sessionID.String())

		ar := &approval.ApprovalRequest{
			SessionID:   sessionID,
			FixActionID: fa.ID,
			Summary:     summary,
			Risk:        fa.Risk,
			Target:      fa.Target,
		}

		token, err := h.approvalClient.RequestApproval(ctx, *ar)
		if err != nil {
			h.log.Error("trigger approval: request failed", "fix_action_id", fa.ID, "error", err)
			continue
		}

		// Persist approval record to DB.
		dbApproval := &store.Approval{
			SessionID:    sessionID,
			FixActionID:  fa.ID,
			Status:       store.ApprovalStatusPending,
			RequestToken: token,
		}
		if err := h.approvalDAO.Create(ctx, dbApproval); err != nil {
			h.log.Error("trigger approval: persist record failed", "fix_action_id", fa.ID, "error", err)
			continue
		}

		// Update fix action approval status.
		h.fixDAO.UpdateApprovalStatus(ctx, fa.ID, store.ApprovalStatusPending)

		h.log.Info("approval gate triggered",
			"session_id", sessionID,
			"fix_action_id", fa.ID,
			"token", token)
	}

	// Send webhook notification for approval required.
	if h.webhookNotifier != nil {
		h.webhookNotifier.Notify(ctx, notify.DiagnosticEvent{
			EventType: "approval_required",
			SessionID: sessionID,
			Status:    string(store.StatusAwaitingApproval),
			Summary:   fmt.Sprintf("Approval required for %d HIGH-risk fix action(s)", len(highRiskFixes)),
			Message:   fmt.Sprintf("Session %s requires approval for %d HIGH-risk fix step(s)", sessionID.String(), len(highRiskFixes)),
			Service:   s.TargetService,
			Timestamp: time.Now().UTC(),
		})
	}

	return nil
}

// --- M6: Fix Execution ---

// ExecuteFixActions executes all approved fix actions for a session.
// It is called after the session transitions to FIX_EXECUTING.
func (h *OrchestratorHandler) ExecuteFixActions(ctx context.Context, sessionID uuid.UUID) error {
	fixes, err := h.fixDAO.GetBySessionID(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("execute fixes: get fixes: %w", err)
	}

	s, err := h.sessionDAO.GetByID(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("execute fixes: get session: %w", err)
	}

	for _, fa := range fixes {
		// Skip actions not yet approved / not requiring approval.
		if fa.RequiresApproval && fa.ApprovalStatus != store.ApprovalStatusApproved {
			continue
		}
		if fa.ExecutionStatus != store.ExecStatusNotStarted {
			continue
		}

		// Mark as running.
		h.fixDAO.UpdateExecutionStatus(ctx, fa.ID, store.ExecStatusRunning)

		execAction := executor.FixAction{
			ID:           fa.ID,
			SessionID:    sessionID,
			ActionType:   fa.ActionType,
			Target:       fa.Target,
			Risk:         fa.Risk,
			RollbackPlan: fa.RollbackPlan,
		}

		result, err := h.executor.Execute(ctx, execAction)
		if err != nil {
			h.log.Error("execute fix: failed",
				"fix_action_id", fa.ID,
				"error", err)
			h.fixDAO.UpdateExecutionStatus(ctx, fa.ID, store.ExecStatusFailed)
			continue
		}

		finalStatus := store.ExecStatusSucceeded
		if result.Status == "FAILED" {
			finalStatus = store.ExecStatusFailed
		}
		h.fixDAO.UpdateExecutionStatus(ctx, fa.ID, finalStatus)

		// Create incident ticket after first succeeded action.
		if result.Status == "SUCCEEDED" && fa.TicketID == nil {
			ticketID, err := h.incidentNotifier.CreateIncident(ctx, notify.Incident{
				SessionID:   sessionID,
				Summary:     fmt.Sprintf("[%s] Auto-fix: %s on %s", s.TargetService, fa.ActionType, fa.Target),
				Description: fmt.Sprintf("Fix action %s executed automatically by microservice-diagnosis.\nRollback plan: %s", fa.ActionType, fa.RollbackPlan),
				Severity:    fa.Risk,
				Status:      "OPEN",
				ReportURL:   fmt.Sprintf("/v1/sessions/%s/report", sessionID.String()),
			})
			if err != nil {
				h.log.Warn("execute fixes: create incident ticket failed", "error", err)
			} else {
				h.fixDAO.SetTicketID(ctx, fa.ID, ticketID)
				h.log.Info("execute fixes: incident ticket created", "ticket_id", ticketID)
			}
		}

		h.log.Info("fix action executed",
			"fix_action_id", fa.ID,
			"status", finalStatus,
			"message", result.Message)
	}

	// Advance to VERIFYING after all actions complete.
	_, err = h.engine.Transition(ctx, sessionID, workflow.EventExecuteComplete)
	if err != nil && !errors.Is(err, workflow.ErrInvalidTransition) {
		return fmt.Errorf("execute fixes: advance to verifying: %w", err)
	}

	// Send notification on fix execution.
	if h.webhookNotifier != nil {
		h.webhookNotifier.Notify(ctx, notify.DiagnosticEvent{
			EventType: "fix_executed",
			SessionID: sessionID,
			Status:    string(store.StatusVerifying),
			Summary:   fmt.Sprintf("Fix actions executed for session %s", sessionID.String()),
			Service:   s.TargetService,
			Timestamp: time.Now().UTC(),
		})
	}

	return nil
}

// NotifySessionEvent sends a webhook notification for a session state change.
func (h *OrchestratorHandler) NotifySessionEvent(ctx context.Context, s *store.DiagnosticSession, eventType, message string) {
	if h.webhookNotifier == nil {
		return
	}
	h.webhookNotifier.Notify(ctx, notify.DiagnosticEvent{
		EventType: eventType,
		SessionID: s.ID,
		Status:    s.Status,
		Summary:   message,
		Service:   s.TargetService,
		Timestamp: time.Now().UTC(),
		Labels:    nil,
	})
}
