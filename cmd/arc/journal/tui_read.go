package journal

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/pkg/reference"
)

// readView owns only UI-local state. Content loading is handled by journalApp.
type readView struct {
	entry    journal.Entry
	content  string
	loading  bool
	viewport viewport.Model
	spinner  spinner.Model
	ready    bool
	layout   *tui.Layout
}

func newReadView(entry journal.Entry, layout *tui.Layout) readView {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)
	return readView{
		entry:   entry,
		loading: true,
		spinner: s,
		layout:  layout,
	}
}

func (v *readView) setContent(content string) {
	v.content = content
	v.loading = false

	vpW, vpH := v.vpSize()
	vp := viewport.New(vpW, vpH)
	vp.SetContent(v.content)
	v.viewport = vp
	v.ready = true
}

func (v readView) vpSize() (int, int) {
	bodyWidth, bodyHeight := 80, 20
	if v.layout != nil {
		bodyWidth, bodyHeight = v.layout.BodySize()
	}
	// Sub-header takes 2 lines (ref+date line, blank line)
	vpH := bodyHeight - 2
	if vpH < 5 {
		vpH = 5
	}
	return bodyWidth, vpH
}

func (v readView) update(msg tea.Msg) (readView, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		v.spinner, cmd = v.spinner.Update(msg)
		return v, cmd
	case tea.WindowSizeMsg:
		if v.ready {
			vpW, vpH := v.vpSize()
			v.viewport.Width = vpW
			v.viewport.Height = vpH
		}
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "backspace", "q":
			return v, func() tea.Msg { return backToListMsg{} }
		}
		if v.ready {
			var cmd tea.Cmd
			v.viewport, cmd = v.viewport.Update(msg)
			return v, cmd
		}
	}
	return v, nil
}

func (v readView) renderSubHeader() string {
	short := reference.Hex(v.entry.Ref)[:8]
	ts := time.Unix(0, v.entry.Timestamp)
	return tui.RefStyle.Render(short) + " " +
		tui.HeaderBarStyle.Render("·") + " " +
		tui.HeaderBarStyle.Render(ts.Format("Jan 2, 2006 15:04"))
}

func (v readView) viewContent() (string, string) {
	helpText := "↑/↓: scroll • q/esc: back"

	var b strings.Builder

	if v.loading {
		b.WriteString(v.renderSubHeader())
		b.WriteString("\n\n")
		b.WriteString(fmt.Sprintf("%s Loading...\n", v.spinner.View()))
		return b.String(), helpText
	}

	b.WriteString(v.renderSubHeader())
	b.WriteString("\n")

	if v.ready {
		b.WriteString(v.viewport.View())
	}

	b.WriteString("\n")

	return b.String(), helpText
}
