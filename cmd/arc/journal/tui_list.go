package journal

import (
	"context"
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

type listModel struct {
	ctx       context.Context
	sdk       *journal.Journal
	entries   []journal.Entry
	cursor    int
	loading   bool
	spinner   spinner.Model
	layout    *tui.Layout
	count     int
	connected bool
	previews  map[reference.Reference]string
}

type entriesLoadedMsg struct {
	entries []journal.Entry
}

type previewLoadedMsg struct {
	ref     reference.Reference
	preview string
}

func newListModel(ctx context.Context, sdk *journal.Journal) listModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)
	return listModel{
		ctx:      ctx,
		sdk:      sdk,
		loading:  true,
		spinner:  s,
		previews: make(map[reference.Reference]string),
	}
}

func (m listModel) Init() tea.Cmd {
	return tea.Batch(m.fetchEntries(), m.spinner.Tick)
}

func (m listModel) fetchEntries() tea.Cmd {
	sdk := m.sdk
	ctx := m.ctx
	return func() tea.Msg {
		result, err := sdk.List(ctx, journal.ListOptions{
			Limit:      50,
			Descending: true,
		})
		if err != nil {
			return errMsg{err: err}
		}
		return entriesLoadedMsg{entries: result.Entries}
	}
}

func (m *listModel) prependEntry(e journal.Entry) {
	for _, existing := range m.entries {
		if existing.Ref == e.Ref {
			return
		}
	}
	m.entries = append([]journal.Entry{e}, m.entries...)
	m.count++
	if m.cursor > 0 {
		m.cursor++
	}
}

func (m listModel) fetchPreviewCmd(e journal.Entry) tea.Cmd {
	sdk := m.sdk
	ctx := m.ctx
	ref := e.Ref
	return func() tea.Msg {
		preview, err := sdk.Preview(ctx, e)
		if err != nil {
			return previewLoadedMsg{ref: ref, preview: ""}
		}
		return previewLoadedMsg{ref: ref, preview: preview}
	}
}

func (m listModel) fetchAllPreviews(entries []journal.Entry) tea.Cmd {
	n := 20
	if len(entries) < n {
		n = len(entries)
	}
	cmds := make([]tea.Cmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = m.fetchPreviewCmd(entries[i])
	}
	return tea.Batch(cmds...)
}

func (m listModel) update(msg tea.Msg) (listModel, tea.Cmd) {
	switch msg := msg.(type) {
	case entriesLoadedMsg:
		m.entries = msg.entries
		m.loading = false
		m.cursor = 0
		return m, m.fetchAllPreviews(msg.entries)
	case previewLoadedMsg:
		m.previews[msg.ref] = msg.preview
		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.KeyMsg:
		switch msg.String() {
		case "j", "down":
			if m.cursor < len(m.entries)-1 {
				m.cursor++
			}
		case "k", "up":
			if m.cursor > 0 {
				m.cursor--
			}
		case "g":
			m.cursor = 0
		case "G":
			if len(m.entries) > 0 {
				m.cursor = len(m.entries) - 1
			}
		case "enter":
			if len(m.entries) > 0 {
				return m, func() tea.Msg {
					return openEntryMsg{entry: m.entries[m.cursor]}
				}
			}
		case "n":
			return m, func() tea.Msg { return openWriteMsg{} }
		case "r":
			m.loading = true
			return m, tea.Batch(m.fetchEntries(), m.spinner.Tick)
		}
	}
	return m, nil
}

func (m listModel) viewContent() (string, string) {
	helpText := "↑/k up • ↓/j down • enter: read • n: new • r: refresh • q: quit"

	bodyWidth, bodyHeight := 80, 20
	if m.layout != nil {
		bodyWidth, bodyHeight = m.layout.BodySize()
	}

	var b strings.Builder

	if m.loading {
		b.WriteString(fmt.Sprintf("%s Loading entries...\n", m.spinner.View()))
		return b.String(), helpText
	}

	if len(m.entries) == 0 {
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

	// Scroll window
	start := 0
	if m.cursor >= visibleCount {
		start = m.cursor - visibleCount + 1
	}
	end := start + visibleCount
	if end > len(m.entries) {
		end = len(m.entries)
		start = end - visibleCount
		if start < 0 {
			start = 0
		}
	}

	visible := m.entries[start:end]
	contentWidth := bodyWidth - 4

	for i, e := range visible {
		idx := start + i
		short := reference.Hex(e.Ref)[:8]
		ts := time.Unix(0, e.Timestamp)
		age := formatDuration(time.Since(ts).Truncate(time.Second)) + " ago"

		if idx == m.cursor {
			b.WriteString(tui.CursorStyle.Render("▸") + " " + tui.RefStyle.Render(short) + "  " + tui.AgeStyle.Render(age))
		} else {
			b.WriteString("  " + tui.RefStyle.Render(short) + "  " + tui.AgeStyle.Render(age))
		}
		b.WriteString("\n")

		if preview, ok := m.previews[e.Ref]; ok && preview != "" {
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
