// Package server provides HTTP server assembly capabilities.
// M0 only includes health checks; M5 will introduce Gin + gRPC-Gateway here.
package server

import (
	"log/slog"
	"net/http"
	"time"
)

// NewHealth builds the basic health-check HTTP server:
//   - /healthz liveness probe
//   - /readyz readiness probe (future milestones will surface dependency readiness)
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

// logRequests is a simple request access log middleware.
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
