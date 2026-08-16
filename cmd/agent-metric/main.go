// agent-metric 服务入口：采集目标服务指标，产出 METRIC 类型 Observation。
package main

import (
	"fmt"
	"os"

	"github.com/microservice-diagnosis/diagnosis-hub/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("agent-metric", bootstrap.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "agent-metric: %v\n", err)
		os.Exit(1)
	}
}
