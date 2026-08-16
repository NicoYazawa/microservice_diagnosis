// agent-rca 服务入口：基于汇聚证据调用 LLM 进行根因分析，产出 RCA_RESULT 类型 Observation。
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
