// agent-trace 服务入口：采集目标服务链路，产出 TRACE 类型 Observation。
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
