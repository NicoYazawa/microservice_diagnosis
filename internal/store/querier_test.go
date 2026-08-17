package store

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type stubTx struct{}

func (stubTx) Begin(context.Context) (pgx.Tx, error) { return nil, nil }
func (stubTx) Commit(context.Context) error           { return nil }
func (stubTx) Rollback(context.Context) error         { return nil }
func (stubTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (stubTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (stubTx) LargeObjects() pgx.LargeObjects                          { return pgx.LargeObjects{} }
func (stubTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (stubTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) { return pgconn.CommandTag{}, nil }
func (stubTx) Query(context.Context, string, ...any) (pgx.Rows, error)          { return nil, nil }
func (stubTx) QueryRow(context.Context, string, ...any) pgx.Row                  { return nil }
func (stubTx) Conn() *pgx.Conn                                                    { return nil }

func TestDAOWithTxUsesTransactionQuerier(t *testing.T) {
	stub := stubTx{}
	webhookDAO := NewWebhookDAO(nil).WithTx(stub)
	fixDAO := NewFixActionDAO(nil).WithTx(stub)
	approvalDAO := NewApprovalDAO(nil).WithTx(stub)
	knowledgeBaseDAO := NewKnowledgeBaseDAO(nil).WithTx(stub)

	if webhookDAO == nil || fixDAO == nil || approvalDAO == nil || knowledgeBaseDAO == nil {
		t.Fatal("WithTx should return non-nil DAOs")
	}
	if _, ok := webhookDAO.db.(stubTx); !ok {
		t.Fatal("WebhookDAO should be bound to the supplied transaction")
	}
	if _, ok := fixDAO.db.(stubTx); !ok {
		t.Fatal("FixActionDAO should be bound to the supplied transaction")
	}
	if _, ok := approvalDAO.db.(stubTx); !ok {
		t.Fatal("ApprovalDAO should be bound to the supplied transaction")
	}
	if _, ok := knowledgeBaseDAO.db.(stubTx); !ok {
		t.Fatal("KnowledgeBaseDAO should be bound to the supplied transaction")
	}
}
