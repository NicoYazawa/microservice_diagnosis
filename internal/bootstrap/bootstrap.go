// Package bootstrap 提供服务标准启动流程：加载配置 → 初始化日志 → 启动健康检查 → 优雅退出。
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

	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
	"github.com/NicoYazawa/microservice_diagnosis/internal/logger"
	"github.com/NicoYazawa/microservice_diagnosis/internal/server"
)

// Options 预留：后续里程碑在此注入 bus / store / workflow 等依赖。
type Options struct{}

// Run 启动一个服务的标准流程。
// serviceName 同时用于默认配置路径（configs/<name>.yaml）与日志 service 字段。
func Run(serviceName string, _ Options) error {
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

	srv := server.NewHealth(cfg.Service.HTTPAddr, log)
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	log.Info("service started", "http_addr", cfg.Service.HTTPAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		return fmt.Errorf("http server failed: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	log.Info("service stopped")
	return nil
}
