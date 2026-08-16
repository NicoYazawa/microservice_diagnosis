// agent-fix service entrypoint: queries the knowledge base based on RCA results
// to generate fix suggestions, emitting FIX_ACTION type Observations.
package main

import (
	"fmt"
	"os"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("agent-fix", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "agent-fix: %v\n", err)
		os.Exit(1)
	}
}
