// agent-rca service entrypoint: performs root cause analysis,
// emitting RCA_RESULT type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	opts := bootstrap.Options{AgentKind: "rca", SkipDatabase: true}
	if err := bootstrap.Run("agent-rca", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-rca: %v\n", err)
		os.Exit(1)
	}
}
