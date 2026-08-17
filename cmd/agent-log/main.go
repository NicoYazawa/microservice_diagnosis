// agent-log service entrypoint: collects logs from the target service,
// emitting LOG type Observations.
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
		AgentKind:   "agent-log",
		SkipDatabase: true,
		OnAgentReady: func(ctx context.Context, cfg *config.Config, log *slog.Logger) {
			if err := agent.Run("agent-log", cfg, log); err != nil {
				log.Error("agent-log exited with error", "error", err)
				os.Exit(1)
			}
		},
	}
	if err := bootstrap.Run("agent-log", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-log: %v\n", err)
		os.Exit(1)
	}
}
