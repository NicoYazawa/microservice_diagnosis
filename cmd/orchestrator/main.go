// Orchestrator service entrypoint: schedules the diagnostic session lifecycle
// (state machine, task dispatch).
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("orchestrator", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: %v\n", err)
		os.Exit(1)
	}
}
