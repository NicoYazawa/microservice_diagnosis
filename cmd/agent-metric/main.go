// agent-metric service entrypoint: collects metrics from the target service,
// emitting METRIC type Observations.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/agent"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
)

func main() {
	opts := bootstrap.Options{
		AgentKind:   "agent-metric",
		SkipDatabase: true,
		OnAgentReady: func(ctx context.Context, cfg *config.Config, log *slog.Logger) {
			if err := agent.Run("agent-metric", cfg, log); err != nil {
				log.Error("agent-metric exited with error", "error", err)
				os.Exit(1)
			}
		},
	}
	if err := bootstrap.Run("agent-metric", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-metric: %v\n", err)
		os.Exit(1)
	}
}
