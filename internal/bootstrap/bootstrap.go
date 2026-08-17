// Package bootstrap provides the standard service startup flow: load config ->
// init logger -> connect to data stores -> register with Consul -> start HTTP
// server -> graceful shutdown.
package bootstrap

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
	"github.com/NicoYazawa/microservice_diagnosis/internal/discovery"
	"github.com/NicoYazawa/microservice_diagnosis/internal/logger"
	"github.com/NicoYazawa/microservice_diagnosis/internal/server"
	"github.com/NicoYazawa/microservice_diagnosis/internal/workflow"
)

// Options holds optional configuration for service startup.
type Options struct {
	// AgentKind is set by agent entrypoints to register as a specific kind in Consul.
	// e.g. "log", "metric", "trace", "rca", "fix"
	AgentKind string
	// SkipConsul disables Consul registration (used for local development without Consul).
	SkipConsul bool
	// SkipDatabase skips PostgreSQL connection (used for purely standalone agents).
	SkipDatabase bool
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

	// Build HTTP server.
	var srv *server.GinServer
	if pool != nil && engine != nil {
		srv = server.NewGinServer(cfg, log, pool, engine, registry)
	} else {
		srv = server.NewGinServer(cfg, log, nil, nil, registry)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	log.Info("service started",
		"name", serviceName,
		"http_addr", cfg.Service.HTTPAddr,
		"consul", cfg.Consul.Addr,
		"kafka", cfg.Bus.Brokers,
	)

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		return fmt.Errorf("http server failed: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}

	if registry != nil {
		if err := registry.Deregister(context.Background(), fmt.Sprintf("%s-%d", serviceName, os.Getpid())); err != nil {
			log.Warn("consul deregister failed", "error", err)
		}
	}

	log.Info("service stopped")
	return nil
}
