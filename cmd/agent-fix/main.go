// agent-fix service entrypoint: generates fix recommendations and optionally executes them,
// emitting FIX_ACTION type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	opts := bootstrap.Options{AgentKind: "fix", SkipDatabase: true}
	if err := bootstrap.Run("agent-fix", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-fix: %v\n", err)
		os.Exit(1)
	}
}
