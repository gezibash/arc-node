package dm

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// chatView owns only UI-local state. Domain data (messages, previews) lives
// in dmApp.
type chatView struct {
	viewport  viewport.Model
	textarea  textarea.Model
	spinner   spinner.Model
	ready     bool
	atBottom  bool
	sending   bool
	loading   bool
	inputRows int

	// Browse mode fields.
	browsing    bool
	msgCursor   int
	newMsgCount int
}

func newChatView(layout *tui.Layout) chatView {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)

	ta := textarea.New()
	ta.Placeholder = "Write a message..."
	ta.Focus()
	ta.CharLimit = 0
	ta.ShowLineNumbers = false
	ta.SetHeight(2)
	width := 72
	if layout != nil {
		w, _ := layout.BodySize()
		if w > 4 {
			width = w - 4
		}
	}
	ta.SetWidth(width)

	return chatView{
		textarea:  ta,
		spinner:   s,
		loading:   true,
		atBottom:  true,
		inputRows: 2,
	}
}

func (v chatView) textareaBlink() tea.Cmd {
	return textarea.Blink
}

func (v chatView) vpSize(layout *tui.Layout) (int, int) {
	bodyWidth, bodyHeight := 80, 20
	if layout != nil {
		bodyWidth, bodyHeight = layout.BodySize()
	}
	vpH := bodyHeight - v.inputRows - 2
	if vpH < 3 {
		vpH = 3
	}
	return bodyWidth, vpH
}

func onResizeChat(v chatView, layout *tui.Layout) chatView {
	if layout != nil {
		w, _ := layout.BodySize()
		if w > 4 {
			v.textarea.SetWidth(w - 4)
		}
	}
	return v
}

func rebuildChatViewport(v chatView, msgs []dm.Message, previews map[reference.Reference]string, self identity.PublicKey, layout *tui.Layout) chatView {
	vpW, vpH := v.vpSize(layout)
	if !v.ready {
		v.viewport = viewport.New(vpW, vpH)
		v.ready = true
	} else {
		v.viewport.Width = vpW
		v.viewport.Height = vpH
	}

	// Track whether we were at bottom before rebuild.
	wasAtBottom := v.atBottom
	v.viewport.SetContent(renderMessages(msgs, previews, self, vpW, v.msgCursor, v.browsing))
	if wasAtBottom {
		v.viewport.GotoBottom()
	}

	// Update atBottom: check if viewport is scrolled to bottom.
	if v.ready {
		v.atBottom = v.viewport.AtBottom()
	}

	return v
}

func renderMessages(msgs []dm.Message, previews map[reference.Reference]string, self identity.PublicKey, width int, cursor int, browsing bool) string {
	if len(msgs) == 0 {
		return tui.PreviewStyle.Render("No messages yet. Start typing below.")
	}

	var b strings.Builder
	for i, msg := range msgs {
		ts := time.UnixMilli(msg.Timestamp)
		timeStr := ts.Format("15:04")
		sender := senderLabel(msg.From, self)

		preview := previews[msg.Ref]
		if preview == "" {
			preview = "(encrypted)"
		}

		line := fmt.Sprintf("%s [%s] %s", timeStr, sender, preview)
		if len(line) > width-2 {
			line = line[:width-5] + "..."
		}
		_ = line

		senderStyle := tui.PeerStyle
		if msg.From == self {
			senderStyle = tui.RefStyle
		}

		prefix := "  "
		if browsing && i == cursor {
			prefix = tui.CursorStyle.Render("▸") + " "
		}

		b.WriteString(prefix + tui.PreviewStyle.Render(timeStr+" ") + senderStyle.Render("["+sender+"]") + " " + preview)
		b.WriteString("\n")
	}
	return b.String()
}

// sendMsg is emitted when the user presses enter with non-empty text.
// dmApp.Update() handles the actual send.
type sendMsg struct{ text string }

// update handles UI-only interactions. Returns updated view and a cmd;
// domain mutations go through dmApp.Update() via messages.
func (v chatView) update(msg tea.Msg, msgs []dm.Message) (chatView, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		v.spinner, cmd = v.spinner.Update(msg)
		return v, cmd

	case tea.KeyMsg:
		if v.browsing {
			return v.updateBrowse(msg, msgs)
		}
		return v.updateCompose(msg)
	}

	if !v.browsing {
		var taCmd tea.Cmd
		v.textarea, taCmd = v.textarea.Update(msg)
		return v, taCmd
	}
	return v, nil
}

func (v chatView) updateCompose(msg tea.KeyMsg) (chatView, tea.Cmd) {
	switch msg.String() {
	case "esc":
		return v, func() tea.Msg { return backToThreadsMsg{} }
	case "enter":
		text := strings.TrimSpace(v.textarea.Value())
		if text == "" || v.sending {
			return v, nil
		}
		v.sending = true
		v.textarea.Reset()
		return v, func() tea.Msg { return sendMsg{text: text} }
	case "ctrl+b":
		v.browsing = true
		v.textarea.Blur()
		return v, nil
	}

	var taCmd tea.Cmd
	v.textarea, taCmd = v.textarea.Update(msg)
	return v, taCmd
}

func (v chatView) updateBrowse(msg tea.KeyMsg, msgs []dm.Message) (chatView, tea.Cmd) {
	switch msg.String() {
	case "esc":
		v.browsing = false
		v.textarea.Focus()
		return v, v.textareaBlink()
	case "j", "down":
		if v.msgCursor < len(msgs)-1 {
			v.msgCursor++
		}
		return v, nil
	case "k", "up":
		if v.msgCursor > 0 {
			v.msgCursor--
		}
		return v, nil
	case "G":
		if len(msgs) > 0 {
			v.msgCursor = len(msgs) - 1
		}
		v.newMsgCount = 0
		v.atBottom = true
		return v, nil
	case "g":
		v.msgCursor = 0
		return v, nil
	case "enter":
		if len(msgs) > 0 && v.msgCursor < len(msgs) {
			m := msgs[v.msgCursor]
			return v, func() tea.Msg { return openMessageMsg{msg: m} }
		}
		return v, nil
	}
	return v, nil
}

func (v chatView) viewContent(_ []dm.Message, _ map[reference.Reference]string, _ identity.PublicKey, layout *tui.Layout) (string, string) {
	var helpText string
	switch {
	case v.sending:
		helpText = "sending..."
	case v.browsing:
		helpText = "j/k: navigate · enter: open · G: latest · esc: compose"
	default:
		helpText = "enter: send · esc: back · ctrl+b: browse"
	}

	var b strings.Builder

	if v.loading {
		b.WriteString(fmt.Sprintf("%s Loading messages...\n", v.spinner.View()))
		return b.String(), helpText
	}

	if v.ready {
		b.WriteString(v.viewport.View())
	}

	// New message indicator.
	if v.newMsgCount > 0 && !v.atBottom {
		indicator := fmt.Sprintf("↓ %d new messages (G to jump)", v.newMsgCount)
		b.WriteString(lipgloss.NewStyle().Foreground(tui.AccentColor).Render(indicator))
		b.WriteString("\n")
	}

	bodyWidth := 80
	if layout != nil {
		bodyWidth, _ = layout.BodySize()
	}
	b.WriteString(tui.PreviewStyle.Render(strings.Repeat("─", bodyWidth)))
	b.WriteString("\n")

	if v.sending {
		b.WriteString(tui.PreviewStyle.Render("Sending..."))
	} else {
		b.WriteString(v.textarea.View())
	}
	b.WriteString("\n")

	return b.String(), helpText
}
