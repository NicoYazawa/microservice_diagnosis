package agent

import (
	"testing"

	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
)

func TestFixAgent_assessRisk(t *testing.T) {
	// FixAgent.assessRisk is a method on the pointer receiver;
	// we can test it in isolation without a real database.
	a := &FixAgent{}

	tests := []struct {
		name       string
		step       FixStep
		wantRisk   string
		wantRollback bool
	}{
		{
			name: "restart_pod is LOW risk",
			step: FixStep{ActionType: "restart_pod", Target: "order-service-abc", RollbackPlan: "auto-restart"},
			wantRisk: store.RiskLow,
		},
		{
			name: "scale_up is LOW risk",
			step: FixStep{ActionType: "scale_up", Target: "replicas=+2", RollbackPlan: "scale down"},
			wantRisk: store.RiskLow,
		},
		{
			name: "scale_down is MEDIUM risk",
			step: FixStep{ActionType: "scale_down", Target: "replicas=-1", RollbackPlan: "scale up"},
			wantRisk: store.RiskMedium,
		},
		{
			name: "switch_master is HIGH risk",
			step: FixStep{ActionType: "switch_master", Target: "db-master", RollbackPlan: "switch back"},
			wantRisk: store.RiskHigh,
		},
		{
			name: "data_migration is HIGH risk",
			step: FixStep{ActionType: "data_migration", Target: "user-table", RollbackPlan: "restore backup"},
			wantRisk: store.RiskHigh,
		},
		{
			name: "config_change is MEDIUM risk",
			step: FixStep{ActionType: "config_change", Target: "max_connections", RollbackPlan: "restore"},
			wantRisk: store.RiskMedium,
		},
		{
			name: "unknown action_type is HIGH risk (fail-closed)",
			step: FixStep{ActionType: "unknown_action", Target: "something", RollbackPlan: ""},
			wantRisk: store.RiskHigh,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			risk, rollback := a.assessRisk(tt.step)
			if risk != tt.wantRisk {
				t.Errorf("assessRisk()[0] = %q, want %q", risk, tt.wantRisk)
			}
			if tt.wantRollback && rollback == "" {
				t.Errorf("assessRisk()[1] = %q, want non-empty rollback", rollback)
			}
		})
	}
}

func TestFixAgent_defaultFixSteps(t *testing.T) {
	a := &FixAgent{}

	tests := []struct {
		pattern string
		wantLen int
	}{
		{"database_connection_pool_exhaustion", 2},
		{"timeout_cascade", 2},
		{"n_plus_one_query", 2},
		{"resource_leak", 2},
		{"unknown_pattern", 1}, // fallback: single restart_pod
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			steps := a.defaultFixSteps(tt.pattern)
			if len(steps) != tt.wantLen {
				t.Errorf("defaultFixSteps(%q) returned %d steps, want %d", tt.pattern, len(steps), tt.wantLen)
			}
			for _, step := range steps {
				if step.ActionType == "" {
					t.Errorf("defaultFixSteps(%q): step has empty ActionType", tt.pattern)
				}
				if step.RollbackPlan == "" {
					t.Errorf("defaultFixSteps(%q): step has empty RollbackPlan", tt.pattern)
				}
			}
		})
	}
}
