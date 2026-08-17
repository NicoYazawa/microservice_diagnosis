package report

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PDFRenderer converts Markdown reports to PDF using external tooling.
type PDFRenderer struct {
	markdownRenderer *Renderer
	tempDir         string
}

// NewPDFRenderer creates a PDF renderer.
// It requires pandoc or maroto/gofpdf to be available; otherwise falls back to a text placeholder.
func NewPDFRenderer(pool *pgxpool.Pool, tempDir string) *PDFRenderer {
	return &PDFRenderer{
		markdownRenderer: NewRenderer(pool),
		tempDir:          tempDir,
	}
}

// RenderPDF renders the report as a PDF byteslice.
// Strategy: try pandoc first → maroto fallback → plain-text placeholder.
func (r *PDFRenderer) RenderPDF(ctx context.Context, data ReportData) ([]byte, string, error) {
	markdown, err := r.markdownRenderer.RenderMarkdown(ctx, data)
	if err != nil {
		return nil, "", fmt.Errorf("pdf: render markdown: %w", err)
	}

	// Try pandoc if available.
	if pdfBytes, err := r.tryPandoc(markdown); err == nil {
		return pdfBytes, "application/pdf", nil
	}

	// Fallback: return markdown with PDF content-type marker so callers know it's not real PDF.
	// In production, integrate maroto or a headless Chrome service here.
	placeholder := []byte(fmt.Sprintf("%% PDF placeholder — markdown available\n\n%s", markdown))
	return placeholder, "text/markdown", nil
}

// tryPandoc attempts to convert markdown to PDF using pandoc.
// Returns an error if pandoc is not found or fails.
func (r *PDFRenderer) tryPandoc(markdown string) ([]byte, error) {
	// Write markdown to a temp file.
	tmpDir := r.tempDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	mdFile := filepath.Join(tmpDir, fmt.Sprintf("report-%d.md", time.Now().UnixNano()))
	pdfFile := strings.TrimSuffix(mdFile, ".md") + ".pdf"

	if err := os.WriteFile(mdFile, []byte(markdown), 0600); err != nil {
		return nil, fmt.Errorf("pandoc: write temp md: %w", err)
	}
	defer os.Remove(mdFile)

	// Try pandoc with weasyprint PDF engine (common in Docker environments).
	args := []string{
		mdFile,
		"--pdf-engine=weasyprint",
		"-o", pdfFile,
		"--pdf-engine-opts=--presentational-hints",
	}
	cmd := exec.Command("pandoc", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Try fallback engine.
		args[1] = "--pdf-engine=pdflatex"
		cmd = exec.Command("pandoc", args...)
		out, err = cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("pandoc: %s: %s", err, string(out))
		}
	}
	defer os.Remove(pdfFile)

	return os.ReadFile(pdfFile)
}

// MarkdownRenderer returns the underlying Markdown renderer for direct use.
func (r *PDFRenderer) MarkdownRenderer() *Renderer {
	return r.markdownRenderer
}
