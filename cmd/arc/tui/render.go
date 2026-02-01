package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/reflow/wordwrap"
)

// RenderContent word-wraps text to fit within the given width and applies
// the content style. No heavyweight markdown rendering — just clean wrapping.
func RenderContent(text string, width int) string {
	w := width - 4
	if w < 40 {
		w = 40
	}
	wrapped := wordwrap.String(text, w)
	return ContentStyle.Render(wrapped)
}

// LooksLikeMarkdown checks whether any line starts with a markdown block marker.
func LooksLikeMarkdown(text string) bool {
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "# ") ||
			strings.HasPrefix(trimmed, "## ") ||
			strings.HasPrefix(trimmed, "### ") ||
			strings.HasPrefix(trimmed, "* ") ||
			strings.HasPrefix(trimmed, "- ") ||
			strings.HasPrefix(trimmed, "> ") ||
			strings.HasPrefix(trimmed, "```") ||
			strings.HasPrefix(trimmed, "---") ||
			strings.HasPrefix(trimmed, "|") {
			return true
		}
	}
	return false
}

// ContentWidth returns a usable content width with padding accounted for.
func ContentWidth(width int) int {
	w := width - 4
	if w < 40 {
		w = 40
	}
	return w
}

// Separator renders a dim horizontal rule.
func Separator(width int) string {
	return lipgloss.NewStyle().Foreground(DimColor).Render(strings.Repeat("─", ContentWidth(width)))
}
