// agent-log 服务入口：采集目标服务日志，产出 LOG 类型 Observation。
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
