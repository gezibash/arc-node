package dm

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// threadsView owns only UI-local state. Domain data (threadList, previews)
// lives in dmApp.
type threadsView struct {
	cursor    int
	loading   bool
	spinner   spinner.Model
	prompting bool
	input     textinput.Model
}

func newThreadsView() threadsView {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)

	ti := textinput.New()
	ti.Placeholder = "peer pubkey (64 hex chars)"
	ti.CharLimit = 64
	ti.Width = 64

	return threadsView{
		loading: true,
		spinner: s,
		input:   ti,
	}
}

func (v threadsView) update(msg tea.Msg, threads []dm.Thread) (threadsView, tea.Cmd) {
	if v.prompting {
		return v.updatePrompt(msg)
	}

	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		v.spinner, cmd = v.spinner.Update(msg)
		return v, cmd
	case tea.KeyMsg:
		switch msg.String() {
		case "j", "down":
			if v.cursor < len(threads)-1 {
				v.cursor++
			}
		case "k", "up":
			if v.cursor > 0 {
				v.cursor--
			}
		case "g":
			v.cursor = 0
		case "G":
			if len(threads) > 0 {
				v.cursor = len(threads) - 1
			}
		case "enter":
			if len(threads) > 0 {
				th := threads[v.cursor]
				return v, func() tea.Msg { return openThreadMsg{thread: th} }
			}
		case "n":
			v.prompting = true
			v.input.Reset()
			v.input.Focus()
			return v, textinput.Blink
		case "r":
			v.loading = true
			return v, func() tea.Msg { return refreshThreadsMsg{} }
		}
	}
	return v, nil
}

func (v threadsView) updatePrompt(msg tea.Msg) (threadsView, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			v.prompting = false
			return v, nil
		case "enter":
			val := strings.TrimSpace(v.input.Value())
			if len(val) != 64 {
				return v, nil
			}
			peerBytes, err := hex.DecodeString(val)
			if err != nil || len(peerBytes) != 32 {
				return v, nil
			}
			var peerPub identity.PublicKey
			copy(peerPub[:], peerBytes)
			v.prompting = false
			return v, func() tea.Msg {
				return openThreadMsg{thread: dm.Thread{PeerPub: peerPub}}
			}
		}
	}
	var cmd tea.Cmd
	v.input, cmd = v.input.Update(msg)
	return v, cmd
}

func (v threadsView) viewContent(threads []dm.Thread, self identity.PublicKey, _ bool, layout *tui.Layout) (string, string) {
	if v.prompting {
		var b strings.Builder
		b.WriteString(tui.SubtitleStyle.Render("new conversation"))
		b.WriteString("\n\n")
		b.WriteString("Peer public key:\n")
		b.WriteString(v.input.View())
		b.WriteString("\n")
		return b.String(), "enter: open • esc: cancel"
	}

	helpText := "↑/k up • ↓/j down • enter: open • n: new • r: refresh • q: quit"

	bodyWidth, bodyHeight := 80, 20
	if layout != nil {
		bodyWidth, bodyHeight = layout.BodySize()
	}

	var b strings.Builder

	if v.loading {
		b.WriteString(fmt.Sprintf("%s Loading threads...\n", v.spinner.View()))
		return b.String(), helpText
	}

	if len(threads) == 0 {
		b.WriteString("No conversations yet. Press n to start one.\n")
		return b.String(), helpText
	}

	linesPerThread := 2
	visibleCount := bodyHeight / linesPerThread
	if visibleCount < 1 {
		visibleCount = 1
	}

	start := 0
	if v.cursor >= visibleCount {
		start = v.cursor - visibleCount + 1
	}
	end := start + visibleCount
	if end > len(threads) {
		end = len(threads)
		start = end - visibleCount
		if start < 0 {
			start = 0
		}
	}

	visible := threads[start:end]
	_ = bodyWidth

	for i, th := range visible {
		idx := start + i
		peerShort := hex.EncodeToString(th.PeerPub[:4])
		ts := time.UnixMilli(th.LastMsg.Timestamp)
		age := formatDuration(time.Since(ts).Truncate(time.Second)) + " ago"

		if idx == v.cursor {
			b.WriteString(tui.CursorStyle.Render("▸") + " " + tui.RefStyle.Render(peerShort) + "  " + tui.AgeStyle.Render(age))
		} else {
			b.WriteString("  " + tui.RefStyle.Render(peerShort) + "  " + tui.AgeStyle.Render(age))
		}
		b.WriteString("\n")

		sender := senderLabel(th.LastMsg.From, self)
		preview := sender + ": (encrypted)"
		if th.Preview != "" {
			firstLine := strings.SplitN(th.Preview, "\n", 2)[0]
			preview = sender + ": " + firstLine
		}
		maxLen := 80
		if len(preview) > maxLen {
			preview = preview[:maxLen] + "…"
		}
		b.WriteString("    " + tui.PreviewStyle.Render(preview))
		b.WriteString("\n")
	}

	return b.String(), helpText
}
