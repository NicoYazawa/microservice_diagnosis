// agent-rca service entrypoint: performs root cause analysis,
// emitting RCA_RESULT type Observations.
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
		AgentKind:   "agent-rca",
		SkipDatabase: true,
		OnAgentReady: func(ctx context.Context, cfg *config.Config, log *slog.Logger) {
			if err := agent.Run("agent-rca", cfg, log); err != nil {
				log.Error("agent-rca exited with error", "error", err)
				os.Exit(1)
			}
		},
	}
	if err := bootstrap.Run("agent-rca", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-rca: %v\n", err)
		os.Exit(1)
	}
}
