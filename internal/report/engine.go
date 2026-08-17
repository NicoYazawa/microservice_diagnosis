package report

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Engine provides the high-level report API used by the HTTP handlers.
type Engine struct {
	pool         *pgxpool.Pool
	renderer     *Renderer
	pdfRenderer  *PDFRenderer
}

// NewEngine creates a report engine.
func NewEngine(pool *pgxpool.Pool) *Engine {
	r := NewRenderer(pool)
	return &Engine{
		pool:         pool,
		renderer:     r,
		pdfRenderer:  NewPDFRenderer(pool, ""),
	}
}

// GenerateReport produces a full diagnostic report for a session.
// The Markdown content is always returned; PDF bytes are returned separately.
func (e *Engine) GenerateReport(ctx context.Context, sessionID uuid.UUID) (markdown string, pdf []byte, contentType string, err error) {
	data, err := e.renderer.LoadReportData(ctx, sessionID)
	if err != nil {
		return "", nil, "", fmt.Errorf("report engine: load data: %w", err)
	}

	markdown, err = e.renderer.RenderMarkdown(ctx, data)
	if err != nil {
		return "", nil, "", fmt.Errorf("report engine: render markdown: %w", err)
	}

	pdf, contentType, err = e.pdfRenderer.RenderPDF(ctx, data)
	if err != nil {
		// Non-fatal: log and continue returning markdown.
		markdown += fmt.Sprintf("\n\n_[PDF generation failed: %v]_\n", err)
		contentType = "text/markdown"
	}

	return markdown, pdf, contentType, nil
}

// RenderMarkdown renders only the markdown version.
func (e *Engine) RenderMarkdown(ctx context.Context, sessionID uuid.UUID) (string, error) {
	data, err := e.renderer.LoadReportData(ctx, sessionID)
	if err != nil {
		return "", err
	}
	return e.renderer.RenderMarkdown(ctx, data)
}
