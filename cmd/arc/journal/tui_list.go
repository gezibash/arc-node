package journal

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/journal"
)

// listView owns only UI-local state. Domain data (entries, previews) lives
// in journalApp.
type listView struct {
	cursor  int
	loading bool
	spinner spinner.Model
}

func newListView() listView {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)
	return listView{
		loading: true,
		spinner: s,
	}
}

func (v listView) update(msg tea.Msg, entries []journal.Entry) (listView, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		v.spinner, cmd = v.spinner.Update(msg)
		return v, cmd
	case tea.KeyMsg:
		switch msg.String() {
		case "j", "down":
			if v.cursor < len(entries)-1 {
				v.cursor++
			}
		case "k", "up":
			if v.cursor > 0 {
				v.cursor--
			}
		case "g":
			v.cursor = 0
		case "G":
			if len(entries) > 0 {
				v.cursor = len(entries) - 1
			}
		case "enter":
			if len(entries) > 0 {
				return v, func() tea.Msg {
					return openEntryMsg{entry: entries[v.cursor]}
				}
			}
		case "n":
			return v, func() tea.Msg { return openWriteMsg{} }
		case "r":
			v.loading = true
			return v, func() tea.Msg { return refreshEntriesMsg{} }
		}
	}
	return v, nil
}

func (v listView) viewContent(entries []journal.Entry, previews map[reference.Reference]string, layout *tui.Layout) (string, string) {
	helpText := "↑/k up • ↓/j down • enter: read • n: new • r: refresh • q: quit"

	bodyWidth, bodyHeight := 80, 20
	if layout != nil {
		bodyWidth, bodyHeight = layout.BodySize()
	}

	var b strings.Builder

	if v.loading {
		b.WriteString(fmt.Sprintf("%s Loading entries...\n", v.spinner.View()))
		return b.String(), helpText
	}

	if len(entries) == 0 {
		b.WriteString("No journal entries yet.\n")
		return b.String(), helpText
	}

	linesPerEntry := 2
	availableLines := bodyHeight
	if availableLines < 3 {
		availableLines = 3
	}
	visibleCount := availableLines / linesPerEntry
	if visibleCount < 1 {
		visibleCount = 1
	}

	start := 0
	if v.cursor >= visibleCount {
		start = v.cursor - visibleCount + 1
	}
	end := start + visibleCount
	if end > len(entries) {
		end = len(entries)
		start = end - visibleCount
		if start < 0 {
			start = 0
		}
	}

	visible := entries[start:end]
	contentWidth := bodyWidth - 4

	for i, e := range visible {
		idx := start + i
		short := reference.Hex(e.Ref)[:8]
		ts := time.Unix(0, e.Timestamp)
		age := formatDuration(time.Since(ts).Truncate(time.Second)) + " ago"

		if idx == v.cursor {
			b.WriteString(tui.CursorStyle.Render("▸") + " " + tui.RefStyle.Render(short) + "  " + tui.AgeStyle.Render(age))
		} else {
			b.WriteString("  " + tui.RefStyle.Render(short) + "  " + tui.AgeStyle.Render(age))
		}
		b.WriteString("\n")

		if preview, ok := previews[e.Ref]; ok && preview != "" {
			firstLine := strings.SplitN(preview, "\n", 2)[0]
			maxLen := contentWidth
			if maxLen > 80 {
				maxLen = 80
			}
			if len(firstLine) > maxLen {
				firstLine = firstLine[:maxLen] + "…"
			}
			b.WriteString("    " + tui.PreviewStyle.Render(firstLine))
		}
		b.WriteString("\n")
	}

	return b.String(), helpText
}
