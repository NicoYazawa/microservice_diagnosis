// Package server provides HTTP server setup with Gin + gRPC-gateway.
package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/NicoYazawa/microservice_diagnosis/api/gen/orchestrator/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/approval"
	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
	"github.com/NicoYazawa/microservice_diagnosis/internal/discovery"
	"github.com/NicoYazawa/microservice_diagnosis/internal/executor"
	"github.com/NicoYazawa/microservice_diagnosis/internal/notify"
	"github.com/NicoYazawa/microservice_diagnosis/internal/report"
	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
	"github.com/NicoYazawa/microservice_diagnosis/internal/workflow"
)

// GinServer is the HTTP server that serves REST via Gin.
type GinServer struct {
	httpServer   *http.Server
	cfg          *config.Config
	log          *slog.Logger
	reportEngine *report.Engine
}

// NewGinServer creates a Gin-based HTTP server with REST handlers wired to the data layer.
func NewGinServer(
	cfg *config.Config,
	log *slog.Logger,
	pool *pgxpool.Pool,
	engine *workflow.Engine,
	registry discovery.Registry,
) *GinServer {
	reportEngine := report.NewEngine(pool)
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Build DAOs.
	sessionDAO := store.NewSessionDAO(pool)
	fixDAO := store.NewFixActionDAO(pool)
	approvalDAO := store.NewApprovalDAO(pool)
	webhookDAO := notify.NewWebhookDAO(pool)

	// Build M6 components from config.
	approvalClient := buildApprovalClient(cfg, log)
	exec := buildExecutor(cfg, log)
	incidentNotifier := buildIncidentNotifier(cfg)
	webhookNotifier := notify.NewWebhookNotifier(webhookDAO, log)

	handler := NewOrchestratorHandler(
		sessionDAO, fixDAO, approvalDAO, webhookDAO,
		engine, registry, approvalClient, exec, incidentNotifier, webhookNotifier, reportEngine, log,
	)

	// REST routes.
	r.GET("/healthz", handler.Healthz)
	// Serve web dashboard (M7).
	r.GET("/", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.File("web/index.html")
	})
	r.GET("/dashboard", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.File("web/index.html")
	})
	v1 := r.Group("/v1")
	{
		v1.POST("/sessions", handler.CreateSession)
		v1.GET("/sessions", handler.ListSessions)
		v1.GET("/sessions/:id", handler.GetSession)
		v1.POST("/sessions/:id/start", handler.StartSession)
		v1.POST("/sessions/:id/retry", handler.RetrySession)
		v1.POST("/sessions/:id/ignore", handler.IgnoreSession)
		v1.POST("/sessions/:id/approvals/:approval_id/decision", handler.DecisionApproval)
		v1.GET("/sessions/:id/report", handler.GetReport)
		v1.GET("/sessions/:id/report/download", handler.DownloadReport)
		v1.GET("/agents", handler.ListAgents)
	}

	// gRPC-gateway: proxy REST calls to a backend gRPC server.
	// Useful when gRPC service and HTTP server are separate processes.
	if cfg.Service.GRPCAddr != "" {
		go func() {
			conn, err := grpc.NewClient(cfg.Service.GRPCAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn("grpc-gateway: dial failed, skipping", "addr", cfg.Service.GRPCAddr, "error", err)
				return
			}
			defer conn.Close()

			mux := runtime.NewServeMux()
			if err := orchestratorv1.RegisterOrchestratorHandlerClient(context.Background(), mux, orchestratorv1.NewOrchestratorClient(conn)); err != nil {
				log.Warn("grpc-gateway: handler registration failed", "error", err)
				return
			}
			// Mount gateway under /v1/grpc to avoid route conflicts with Gin.
			r.Any("/v1/grpc/*path", gin.WrapH(mux))
			log.Info("grpc-gateway mounted", "backend", cfg.Service.GRPCAddr)
		}()
	}

	return &GinServer{
		httpServer: &http.Server{
			Addr:         cfg.Service.HTTPAddr,
			Handler:      r,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		cfg:          cfg,
		log:          log,
		reportEngine: reportEngine,
	}
}

// --- M6 component builders ---

func buildApprovalClient(cfg *config.Config, log *slog.Logger) approval.ApprovalClient {
	switch cfg.Approval.Mode {
	case "webhook":
		return approval.NewWebhookClient(cfg.Approval.Callback, nil)
	case "noop":
		return approval.NewNOOPClient()
	default:
		log.Warn("approval mode unknown, using NOOP", "mode", cfg.Approval.Mode)
		return approval.NewNOOPClient()
	}
}

func buildExecutor(cfg *config.Config, log *slog.Logger) executor.Executor {
	switch cfg.Fix.Mode {
	case "k8s":
		exec, err := executor.NewK8sExecutor(cfg.Fix.Kubeconfig, log)
		if err != nil {
			log.Warn("k8s executor init failed, falling back to NOOP", "error", err)
			return executor.NewNOOPExecutor()
		}
		return exec
	case "noop", "":
		return executor.NewNOOPExecutor()
	default:
		log.Warn("fix mode unknown, using NOOP", "mode", cfg.Fix.Mode)
		return executor.NewNOOPExecutor()
	}
}

func buildIncidentNotifier(cfg *config.Config) notify.IncidentNotifier {
	switch cfg.Notify.IncidentNotifier {
	case "jira":
		return notify.NewJiraIncidentNotifier(
			cfg.Notify.JiraBaseURL,
			cfg.Notify.JiraAuthToken,
			cfg.Notify.JiraProjectKey,
		)
	case "pagerduty":
		return notify.NewPagerDutyIncidentNotifier(cfg.Notify.PDRoutingKey)
	case "noop", "":
		return &notify.NOOPIncidentNotifier{}
	default:
		return &notify.NOOPIncidentNotifier{}
	}
}

// ListenAndServe starts the HTTP server.
func (s *GinServer) ListenAndServe() error {
	s.log.Info("http server listening", "addr", s.cfg.Service.HTTPAddr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *GinServer) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
