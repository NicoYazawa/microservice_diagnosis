// agent-metric service entrypoint: collects metrics from the target service,
// emitting METRIC type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("agent-metric", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "agent-metric: %v\n", err)
		os.Exit(1)
	}
}
