// agent-trace service entrypoint: collects traces from the target service,
// emitting TRACE type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("agent-trace", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "agent-trace: %v\n", err)
		os.Exit(1)
	}
}
