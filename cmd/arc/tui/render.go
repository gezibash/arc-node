package tui

import (
	"strings"

	"github.com/charmbracelet/glamour"
)

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

// RenderContent applies glamour rendering if the text looks like markdown,
// otherwise applies a plain content style.
func RenderContent(text string, width int) string {
	if LooksLikeMarkdown(text) {
		renderWidth := width - 10
		if renderWidth < 40 {
			renderWidth = 40
		}
		renderer, err := glamour.NewTermRenderer(
			glamour.WithAutoStyle(),
			glamour.WithWordWrap(renderWidth),
		)
		if err == nil {
			if rendered, err := renderer.Render(text); err == nil {
				return rendered
			}
		}
	}
	return ContentStyle.Render(text)
}
