// agent-trace service entrypoint: collects traces from the target service,
// emitting TRACE type Observations.
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
		AgentKind:   "agent-trace",
		SkipDatabase: true,
		OnAgentReady: func(ctx context.Context, cfg *config.Config, log *slog.Logger) {
			if err := agent.Run("agent-trace", cfg, log); err != nil {
				log.Error("agent-trace exited with error", "error", err)
				os.Exit(1)
			}
		},
	}
	if err := bootstrap.Run("agent-trace", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-trace: %v\n", err)
		os.Exit(1)
	}
}
