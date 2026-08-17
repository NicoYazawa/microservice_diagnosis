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
	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
	"github.com/NicoYazawa/microservice_diagnosis/internal/discovery"
	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
	"github.com/NicoYazawa/microservice_diagnosis/internal/workflow"
)

// GinServer is the HTTP server that serves REST via Gin.
type GinServer struct {
	httpServer *http.Server
	cfg        *config.Config
	log        *slog.Logger
}

// NewGinServer creates a Gin-based HTTP server with REST handlers wired to the data layer.
func NewGinServer(
	cfg *config.Config,
	log *slog.Logger,
	pool *pgxpool.Pool,
	engine *workflow.Engine,
	registry discovery.Registry,
) *GinServer {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Build DAOs.
	sessionDAO := store.NewSessionDAO(pool)
	fixDAO := store.NewFixActionDAO(pool)
	approvalDAO := store.NewApprovalDAO(pool)

	handler := NewOrchestratorHandler(sessionDAO, fixDAO, approvalDAO, engine, registry, log)

	// REST routes.
	r.GET("/healthz", handler.Healthz)
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
		cfg: cfg,
		log: log,
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
