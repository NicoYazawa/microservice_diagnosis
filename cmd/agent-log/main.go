// agent-log service entrypoint: collects logs from the target service,
// emitting LOG type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	opts := bootstrap.Options{AgentKind: "log", SkipDatabase: true}
	if err := bootstrap.Run("agent-log", opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent-log: %v\n", err)
		os.Exit(1)
	}
}
