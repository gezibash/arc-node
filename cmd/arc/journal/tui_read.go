package journal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/journal"
)

var (
	headerBarStyle = lipgloss.NewStyle().
			Foreground(tui.DimColor)

	contentStyle = lipgloss.NewStyle().
			Foreground(lipgloss.AdaptiveColor{Light: "#1A1A1A", Dark: "#FAFAFA"})
)

type readModel struct {
	ctx      context.Context
	sdk      *journal.Journal
	entry    journal.Entry
	content  string
	loading  bool
	viewport viewport.Model
	spinner  spinner.Model
	ready    bool
	layout   *tui.Layout
}

type contentLoadedMsg struct {
	content string
}

func newReadModel(ctx context.Context, sdk *journal.Journal, entry journal.Entry, layout *tui.Layout) readModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)
	return readModel{
		ctx:     ctx,
		sdk:     sdk,
		entry:   entry,
		loading: true,
		spinner: s,
		layout:  layout,
	}
}

// looksLikeMarkdown checks whether any line starts with a markdown block marker.
func looksLikeMarkdown(text string) bool {
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

func (m readModel) Init() tea.Cmd {
	entry := m.entry
	sdk := m.sdk
	ctx := m.ctx
	width := 80
	if m.layout != nil {
		width = m.layout.Width
	}
	return tea.Batch(
		m.spinner.Tick,
		func() tea.Msg {
			contentHex := entry.Labels["content"]
			if contentHex == "" {
				return errMsg{err: fmt.Errorf("no content label")}
			}
			ref, err := reference.FromHex(contentHex)
			if err != nil {
				return errMsg{err: err}
			}
			result, err := sdk.Read(ctx, ref)
			if err != nil {
				return errMsg{err: err}
			}
			text := string(result.Content)

			if looksLikeMarkdown(text) {
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
						text = rendered
					}
				}
			} else {
				text = contentStyle.Render(text)
			}
			return contentLoadedMsg{content: text}
		},
	)
}

func (m readModel) vpSize() (int, int) {
	bodyWidth, bodyHeight := 80, 20
	if m.layout != nil {
		bodyWidth, bodyHeight = m.layout.BodySize()
	}
	// Sub-header takes 2 lines (ref+date line, blank line)
	vpH := bodyHeight - 2
	if vpH < 5 {
		vpH = 5
	}
	return bodyWidth, vpH
}

func (m readModel) update(msg tea.Msg) (readModel, tea.Cmd) {
	switch msg := msg.(type) {
	case contentLoadedMsg:
		m.content = msg.content
		m.loading = false

		vpW, vpH := m.vpSize()
		vp := viewport.New(vpW, vpH)
		vp.SetContent(m.content)
		m.viewport = vp
		m.ready = true
		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		if m.ready {
			vpW, vpH := m.vpSize()
			m.viewport.Width = vpW
			m.viewport.Height = vpH
		}
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "backspace", "q":
			return m, func() tea.Msg { return backToListMsg{} }
		}
		if m.ready {
			var cmd tea.Cmd
			m.viewport, cmd = m.viewport.Update(msg)
			return m, cmd
		}
	}
	return m, nil
}

func (m readModel) renderSubHeader() string {
	short := reference.Hex(m.entry.Ref)[:8]
	ts := time.Unix(0, m.entry.Timestamp)
	return tui.RefStyle.Render(short) + " " +
		headerBarStyle.Render("·") + " " +
		headerBarStyle.Render(ts.Format("Jan 2, 2006 15:04"))
}

func (m readModel) viewContent() (string, string) {
	helpText := "↑/↓: scroll • q/esc: back"

	var b strings.Builder

	if m.loading {
		b.WriteString(m.renderSubHeader())
		b.WriteString("\n\n")
		b.WriteString(fmt.Sprintf("%s Loading...\n", m.spinner.View()))
		return b.String(), helpText
	}

	b.WriteString(m.renderSubHeader())
	b.WriteString("\n")

	if m.ready {
		b.WriteString(m.viewport.View())
	}

	b.WriteString("\n")

	return b.String(), helpText
}
