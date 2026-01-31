package journal

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/pkg/reference"
)

type writeModel struct {
	ctx      context.Context
	sdk      *journal.Journal
	textarea textarea.Model
	saving   bool
	err      error
	layout   *tui.Layout
}

type writeDoneMsg struct{ ref reference.Reference }

func newWriteModel(ctx context.Context, sdk *journal.Journal, layout *tui.Layout) writeModel {
	ta := textarea.New()
	ta.Placeholder = "Write your journal entry..."
	ta.Focus()
	ta.CharLimit = 0
	width := 72
	if layout != nil {
		w, _ := layout.BodySize()
		if w > 4 {
			width = w - 4
		}
	}
	ta.SetWidth(width)
	ta.SetHeight(15)
	ta.ShowLineNumbers = false

	return writeModel{
		ctx:      ctx,
		sdk:      sdk,
		textarea: ta,
		layout:   layout,
	}
}

func (m writeModel) Init() tea.Cmd {
	return textarea.Blink
}

func (m writeModel) update(msg tea.Msg) (writeModel, tea.Cmd) {
	if m.saving {
		switch msg := msg.(type) {
		case writeDoneMsg:
			m.saving = false
			return m, func() tea.Msg {
				return writeCompleteMsg{ref: msg.ref}
			}
		case errMsg:
			m.err = msg.err
			m.saving = false
			return m, nil
		}
		return m, nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			return m, func() tea.Msg { return backToListMsg{} }
		case "ctrl+s":
			text := strings.TrimSpace(m.textarea.Value())
			if text == "" {
				return m, nil
			}
			m.saving = true
			sdk := m.sdk
			ctx := m.ctx
			return m, func() tea.Msg {
				result, err := sdk.Write(ctx, []byte(text), nil)
				if err != nil {
					return errMsg{err: err}
				}
				return writeDoneMsg{ref: result.Ref}
			}
		}
	case tea.WindowSizeMsg:
		if m.layout != nil {
			w, _ := m.layout.BodySize()
			if w > 6 {
				m.textarea.SetWidth(w - 6)
			}
		}
	}

	var cmd tea.Cmd
	m.textarea, cmd = m.textarea.Update(msg)
	return m, cmd
}

func (m writeModel) viewContent() (string, string) {
	helpText := "ctrl+s: save • esc: cancel"

	var b strings.Builder

	b.WriteString(tui.SubtitleStyle.Render("new entry"))
	b.WriteString("\n\n")

	if m.saving {
		b.WriteString("Encrypting and publishing...\n")
		return b.String(), helpText
	}

	if m.err != nil {
		b.WriteString(tui.ErrorStyle.Render(fmt.Sprintf("Error: %v", m.err)))
		b.WriteString("\n\n")
	}

	b.WriteString(m.textarea.View())
	b.WriteString("\n")

	return b.String(), helpText
}
