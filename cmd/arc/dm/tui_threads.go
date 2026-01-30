package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/dm"
)

type threadsModel struct {
	ctx       context.Context
	threads   *dm.Threads
	self      identity.PublicKey
	items     []dm.Thread
	cursor    int
	loading   bool
	spinner   spinner.Model
	layout    *tui.Layout
	connected bool
	// New conversation prompt
	prompting bool
	input     textinput.Model
}

type threadsLoadedMsg struct {
	threads []dm.Thread
}

type threadPreviewLoadedMsg struct {
	convID  string
	preview string
}

func newThreadsModel(ctx context.Context, threads *dm.Threads, self identity.PublicKey) threadsModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)

	ti := textinput.New()
	ti.Placeholder = "peer pubkey (64 hex chars)"
	ti.CharLimit = 64
	ti.Width = 64

	return threadsModel{
		ctx:     ctx,
		threads: threads,
		self:    self,
		loading: true,
		spinner: s,
		input:   ti,
	}
}

func (m threadsModel) Init() tea.Cmd {
	return tea.Batch(m.fetchThreads(), m.spinner.Tick)
}

func (m threadsModel) fetchThreads() tea.Cmd {
	threads := m.threads
	ctx := m.ctx
	return func() tea.Msg {
		result, err := threads.ListThreads(ctx)
		if err != nil {
			return errMsg{err: err}
		}
		return threadsLoadedMsg{threads: result}
	}
}

func (m threadsModel) fetchAllThreadPreviews() tea.Cmd {
	n := len(m.items)
	if n > 10 {
		n = 10
	}
	cmds := make([]tea.Cmd, n)
	for i := 0; i < n; i++ {
		th := m.items[i]
		threads := m.threads
		ctx := m.ctx
		cmds[i] = func() tea.Msg {
			preview, err := threads.PreviewThread(ctx, th)
			if err != nil {
				return threadPreviewLoadedMsg{convID: th.ConvID, preview: ""}
			}
			return threadPreviewLoadedMsg{convID: th.ConvID, preview: preview}
		}
	}
	return tea.Batch(cmds...)
}

func (m *threadsModel) updateThread(msg dm.Message) {
	convID := msg.Labels["conversation"]
	if convID == "" {
		return
	}

	peer := msg.From
	if msg.From == m.self {
		peer = msg.To
	}

	// Update existing thread or insert new one.
	for i, th := range m.items {
		if th.ConvID == convID {
			m.items[i].LastMsg = msg
			m.items[i].Preview = ""
			// Move to front.
			updated := m.items[i]
			copy(m.items[1:i+1], m.items[:i])
			m.items[0] = updated
			return
		}
	}

	// New thread — prepend.
	m.items = append([]dm.Thread{{
		ConvID:  convID,
		PeerPub: peer,
		LastMsg: msg,
	}}, m.items...)
}

func (m threadsModel) update(msg tea.Msg) (threadsModel, tea.Cmd) {
	if m.prompting {
		return m.updatePrompt(msg)
	}

	switch msg := msg.(type) {
	case threadsLoadedMsg:
		m.items = msg.threads
		m.loading = false
		return m, m.fetchAllThreadPreviews()
	case threadPreviewLoadedMsg:
		for i, th := range m.items {
			if th.ConvID == msg.convID {
				m.items[i].Preview = msg.preview
				break
			}
		}
		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.KeyMsg:
		switch msg.String() {
		case "j", "down":
			if m.cursor < len(m.items)-1 {
				m.cursor++
			}
		case "k", "up":
			if m.cursor > 0 {
				m.cursor--
			}
		case "g":
			m.cursor = 0
		case "G":
			if len(m.items) > 0 {
				m.cursor = len(m.items) - 1
			}
		case "enter":
			if len(m.items) > 0 {
				th := m.items[m.cursor]
				return m, func() tea.Msg { return openThreadMsg{thread: th} }
			}
		case "n":
			m.prompting = true
			m.input.Reset()
			m.input.Focus()
			return m, textinput.Blink
		case "r":
			m.loading = true
			return m, tea.Batch(m.fetchThreads(), m.spinner.Tick)
		}
	}
	return m, nil
}

func (m threadsModel) updatePrompt(msg tea.Msg) (threadsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			m.prompting = false
			return m, nil
		case "enter":
			val := strings.TrimSpace(m.input.Value())
			if len(val) != 64 {
				return m, nil
			}
			peerBytes, err := hex.DecodeString(val)
			if err != nil || len(peerBytes) != 32 {
				return m, nil
			}
			var peerPub identity.PublicKey
			copy(peerPub[:], peerBytes)
			m.prompting = false
			return m, func() tea.Msg {
				return openThreadMsg{thread: dm.Thread{PeerPub: peerPub}}
			}
		}
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m threadsModel) viewContent() (string, string) {
	if m.prompting {
		var b strings.Builder
		b.WriteString(tui.SubtitleStyle.Render("new conversation"))
		b.WriteString("\n\n")
		b.WriteString("Peer public key:\n")
		b.WriteString(m.input.View())
		b.WriteString("\n")
		return b.String(), "enter: open • esc: cancel"
	}

	helpText := "↑/k up • ↓/j down • enter: open • n: new • r: refresh • q: quit"

	bodyWidth, bodyHeight := 80, 20
	if m.layout != nil {
		bodyWidth, bodyHeight = m.layout.BodySize()
	}

	var b strings.Builder

	if m.loading {
		b.WriteString(fmt.Sprintf("%s Loading threads...\n", m.spinner.View()))
		return b.String(), helpText
	}

	if len(m.items) == 0 {
		b.WriteString("No conversations yet. Press n to start one.\n")
		return b.String(), helpText
	}

	linesPerThread := 2
	visibleCount := bodyHeight / linesPerThread
	if visibleCount < 1 {
		visibleCount = 1
	}

	start := 0
	if m.cursor >= visibleCount {
		start = m.cursor - visibleCount + 1
	}
	end := start + visibleCount
	if end > len(m.items) {
		end = len(m.items)
		start = end - visibleCount
		if start < 0 {
			start = 0
		}
	}

	visible := m.items[start:end]
	_ = bodyWidth

	for i, th := range visible {
		idx := start + i
		peerShort := hex.EncodeToString(th.PeerPub[:4])
		ts := time.Unix(0, th.LastMsg.Timestamp)
		age := formatDuration(time.Since(ts).Truncate(time.Second)) + " ago"

		if idx == m.cursor {
			b.WriteString(tui.CursorStyle.Render("▸") + " " + tui.RefStyle.Render(peerShort) + "  " + tui.AgeStyle.Render(age))
		} else {
			b.WriteString("  " + tui.RefStyle.Render(peerShort) + "  " + tui.AgeStyle.Render(age))
		}
		b.WriteString("\n")

		sender := senderLabel(th.LastMsg.From, m.self)
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
