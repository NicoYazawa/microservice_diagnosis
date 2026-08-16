// agent-rca service entrypoint: performs root cause analysis by calling an LLM
// on the aggregated evidence, emitting RCA_RESULT type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("agent-rca", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "agent-rca: %v\n", err)
		os.Exit(1)
	}
}
