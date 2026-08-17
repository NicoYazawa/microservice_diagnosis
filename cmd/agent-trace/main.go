// agent-trace service entrypoint: collects traces from the target service,
// emitting TRACE type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	opts := bootstrap.Options{AgentKind: "trace", SkipDatabase: true}
	if err := bootstrap.Run("agent-trace", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-trace: %v\n", err)
		os.Exit(1)
	}
}
