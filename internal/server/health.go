// Package server 提供 HTTP 服务装配能力。
// M0 阶段仅包含健康检查；M5 里程碑在此引入 Gin + gRPC-Gateway。
package server

import (
	"log/slog"
	"net/http"
	"time"
)

// NewHealth 构造基础健康检查 HTTP 服务：
//   - /healthz 存活探针
//   - /readyz 就绪探针（后续里程碑接入依赖就绪状态）
func NewHealth(addr string, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	return &http.Server{
		Addr:              addr,
		Handler:           logRequests(log, mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// logRequests 简单请求访问日志中间件。
func logRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"duration", time.Since(start).String(),
		)
	})
}
