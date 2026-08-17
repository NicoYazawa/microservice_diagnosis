// Package bootstrap provides the standard service startup flow: load config ->
// init logger -> connect to data stores -> register with Consul -> start HTTP
// server -> graceful shutdown.
package bootstrap

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NicoYazawa/microservice_diagnosis/internal/approval"
	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
	"github.com/NicoYazawa/microservice_diagnosis/internal/discovery"
	"github.com/NicoYazawa/microservice_diagnosis/internal/executor"
	"github.com/NicoYazawa/microservice_diagnosis/internal/logger"
	"github.com/NicoYazawa/microservice_diagnosis/internal/notify"
	"github.com/NicoYazawa/microservice_diagnosis/internal/server"
	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
	"github.com/NicoYazawa/microservice_diagnosis/internal/workflow"
)

// Options holds optional configuration for service startup.
type Options struct {
	// AgentKind is set by agent entrypoints to register as a specific kind in Consul.
	// e.g. "log", "metric", "trace", "rca", "fix"
	AgentKind string
	// SkipConsul disables Consul registry (used for local development without Consul).
	SkipConsul bool
	// SkipDatabase skips PostgreSQL connection (used for purely standalone agents).
	SkipDatabase bool
	// OnOrchestratorReady is called after the HTTP server starts and the workflow engine
	// is available, before blocking on the shutdown select. Used to start background loops
	// (event consumer, sweep) for the orchestrator.
	OnOrchestratorReady func(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, engine *workflow.Engine, log *slog.Logger)
	// OnAgentReady is called after the HTTP server starts for agent services.
	// It receives the Kafka bus config and a logger so the agent can start its consumer/producer.
	OnAgentReady func(ctx context.Context, cfg *config.Config, log *slog.Logger)
}

// Run starts a service using the standard startup flow.
func Run(serviceName string, opts Options) error {
	defaultCfg := fmt.Sprintf("configs/%s.yaml", serviceName)
	configPath := flag.String("config", defaultCfg, "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(logger.Options{
		Level:   cfg.Log.Level,
		Format:  cfg.Log.Format,
		Service: serviceName,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to PostgreSQL.
	var pool *pgxpool.Pool
	if !opts.SkipDatabase {
		poolConfig, err := pgxpool.ParseConfig(cfg.Database.URL)
		if err != nil {
			return fmt.Errorf("parse pg config: %w", err)
		}
		poolConfig.MaxConns = 20
		poolConfig.MinConns = 2

		poolCtx, poolCancel := context.WithTimeout(ctx, 10*time.Second)
		pool, err = pgxpool.NewWithConfig(poolCtx, poolConfig)
		poolCancel()
		if err != nil {
			return fmt.Errorf("pg pool: %w", err)
		}
		defer pool.Close()

		pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
		err = pool.Ping(pingCtx)
		pingCancel()
		if err != nil {
			return fmt.Errorf("pg ping: %w", err)
		}
		log.Info("postgres connected",
			"host", poolConfig.ConnConfig.Host,
			"port", poolConfig.ConnConfig.Port,
			"database", poolConfig.ConnConfig.Database,
		)
	}

	// Build workflow engine (requires pool).
	var engine *workflow.Engine
	if pool != nil {
		engine = workflow.NewEngine(pool, log)
	}

	// Consul registry.
	var registry discovery.Registry
	if !opts.SkipConsul && cfg.Consul.Addr != "" {
		registry, err = discovery.NewConsulRegistry(cfg.Consul.Addr, log)
		if err != nil {
			log.Warn("consul unavailable, running without service registry", "error", err)
			registry = discovery.NewMockRegistry(log)
		} else {
			inst := &discovery.ServiceInstance{
				ID:       fmt.Sprintf("%s-%d", serviceName, os.Getpid()),
				Name:     serviceName,
				Kind:     opts.AgentKind,
				Version:  "1.0.0",
				HTTPAddr: cfg.Service.HTTPAddr,
				Status:   "healthy",
			}
			if err := registry.Register(ctx, inst); err != nil {
				log.Warn("consul registration failed", "error", err)
			} else {
				log.Info("consul registered", "id", inst.ID, "name", serviceName)
			}
		}
	} else {
		registry = discovery.NewMockRegistry(log)
	}

	// Build HTTP server (only for services with a database connection).
	var srv *server.GinServer
	if pool != nil && engine != nil {
		srv = server.NewGinServer(ctx, cfg, log, pool, engine, registry)
	}
	// For agents (SkipDatabase=true), pool is nil so srv stays nil.

	errCh := make(chan error, 1)
	if srv != nil {
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}

	log.Info("service started",
		"name", serviceName,
		"http_addr", cfg.Service.HTTPAddr,
		"consul", cfg.Consul.Addr,
		"kafka", cfg.Bus.Brokers,
	)

	// Fire the orchestrator lifecycle hook after HTTP server is listening.
	// This starts background loops (event consumer, sweep) for the orchestrator.
	if pool != nil && engine != nil && opts.OnOrchestratorReady != nil {
		opts.OnOrchestratorReady(ctx, cfg, pool, engine, log)
	}

	// Fire the agent lifecycle hook so agents can start their Kafka consumer/producer loops.
	if opts.OnAgentReady != nil && opts.AgentKind != "" && opts.AgentKind != "orchestrator" {
		opts.OnAgentReady(ctx, cfg, log)
	}

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		return fmt.Errorf("http server failed: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if srv != nil {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful shutdown failed", "error", err)
		}
	}

	if registry != nil {
		if err := registry.Deregister(context.Background(), fmt.Sprintf("%s-%d", serviceName, os.Getpid())); err != nil {
			log.Warn("consul deregister failed", "error", err)
		}
	}

	log.Info("service stopped")
	return nil
}

// OrchestratorReady is the default OnOrchestratorReady implementation.
// The event loop is already started in NewGinServer (before this callback fires).
// This callback only sets the sessionEventDAO on the engine.
func OrchestratorReady(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, engine *workflow.Engine, log *slog.Logger) {
	sessionEventDAO := store.NewSessionEventDAO(pool)
	engine.SetSessionEventDAO(sessionEventDAO)
	log.Info("orchestrator ready: sessionEventDAO wired", "service", "orchestrator")
}

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
