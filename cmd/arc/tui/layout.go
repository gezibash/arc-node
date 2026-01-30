package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Pulse colors cycle through green brightness levels.
var pulseColors = []lipgloss.Color{
	"#73F59F",
	"#5FE08B",
	"#4BCC77",
	"#3FB86A",
	"#4BCC77",
	"#5FE08B",
}

// Layout provides a shared frame for all TUI views: header, body, footer.
type Layout struct {
	AppName   string
	NodeKey   string // short hex, e.g. "9ef03dbf"
	Connected bool
	Width     int
	Height    int
	Frame     int // incremented on each spinner tick for pulse animation
}

// BodySize returns the available (width, height) for app content.
// Reserves: top pad(1) + header(1) + blank(1) + footer(1) + bottom pad(1) = 5 lines,
// and horizontal padding of 2 on each side = 4 columns.
func (l Layout) BodySize() (int, int) {
	w := l.Width - 4
	if w < 10 {
		w = 10
	}
	h := l.Height - 6
	if h < 3 {
		h = 3
	}
	return w, h
}

// Render composes header + body + footer into a full frame with symmetric padding.
func (l Layout) Render(body string, helpText string) string {
	contentWidth := l.Width - 4
	if contentWidth < 10 {
		contentWidth = 10
	}

	var frame strings.Builder

	// Top padding
	frame.WriteString("\n")

	// Header: "arc · {appName}" left, "arc://{key} ●" right
	left := TitleStyle.Render("arc") +
		lipgloss.NewStyle().Foreground(DimColor).Render(" · ") +
		lipgloss.NewStyle().Foreground(DimColor).Render(l.AppName)

	var right string
	if l.NodeKey != "" {
		dot := lipgloss.NewStyle().Foreground(DimColor).Render("●")
		if l.Connected {
			c := pulseColors[l.Frame%len(pulseColors)]
			dot = lipgloss.NewStyle().Foreground(c).Bold(true).Render("●")
		}
		right = lipgloss.NewStyle().Foreground(DimColor).Render("arc://"+l.NodeKey) + " " + dot
	}

	gap := contentWidth - lipgloss.Width(left) - lipgloss.Width(right) - 1
	if gap < 1 {
		gap = 1
	}
	frame.WriteString("  " + left + strings.Repeat(" ", gap) + right + " ")
	frame.WriteString("\n\n")

	// Body — indent every line by 2 spaces for consistent left padding
	for _, line := range strings.Split(body, "\n") {
		frame.WriteString("  " + line + "\n")
	}

	// Pad body to push footer to bottom
	_, bodyHeight := l.BodySize()
	bodyLines := len(strings.Split(body, "\n"))
	pad := bodyHeight - bodyLines
	if pad < 0 {
		pad = 0
	}
	frame.WriteString(strings.Repeat("\n", pad))

	// Footer
	frame.WriteString(HelpStyle.Render("  " + helpText))
	frame.WriteString("\n")

	return frame.String()
}
