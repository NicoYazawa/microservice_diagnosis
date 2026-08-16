// agent-fix 服务入口：基于 RCA 结果查询知识库生成修复建议，产出 FIX_ACTION 类型 Observation。
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
