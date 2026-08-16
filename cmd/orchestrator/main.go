// Orchestrator 服务入口：负责诊断会话生命周期调度（状态机、任务分发）。
package main

import (
	"fmt"
	"os"

	"github.com/microservice-diagnosis/diagnosis-hub/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("orchestrator", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: %v\n", err)
		os.Exit(1)
	}
}
