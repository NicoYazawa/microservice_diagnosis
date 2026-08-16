// agent-log service entrypoint: collects logs from the target service,
// emitting LOG type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("agent-log", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "agent-log: %v\n", err)
		os.Exit(1)
	}
}
