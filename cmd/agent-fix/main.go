// agent-fix service entrypoint: proposes and executes fix actions,
// emitting FIX_ACTION type Observations.
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
		AgentKind:   "agent-fix",
		SkipDatabase: true,
		OnAgentReady: func(ctx context.Context, cfg *config.Config, log *slog.Logger) {
			if err := agent.Run("agent-fix", cfg, log); err != nil {
				log.Error("agent-fix exited with error", "error", err)
				os.Exit(1)
			}
		},
	}
	if err := bootstrap.Run("agent-fix", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-fix: %v\n", err)
		os.Exit(1)
	}
}
