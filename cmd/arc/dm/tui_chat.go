package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/dm"
)

type chatModel struct {
	ctx       context.Context
	sdk       *dm.DM
	self      identity.PublicKey
	peer      identity.PublicKey
	messages  []dm.Message
	previews  map[reference.Reference]string
	viewport  viewport.Model
	textarea  textarea.Model
	spinner   spinner.Model
	layout    *tui.Layout
	loading   bool
	ready     bool
	sending   bool
	atBottom  bool
	sub       <-chan *dm.Message
	subErr    <-chan error
	inputRows int
}

type chatMessagesLoadedMsg struct {
	messages []dm.Message
}

type chatPreviewLoadedMsg struct {
	ref     reference.Reference
	preview string
}

type chatSubStartedMsg struct {
	msgs <-chan *dm.Message
	errs <-chan error
}

type chatNewMessageMsg struct{ msg dm.Message }
type chatSubErrorMsg struct{ err error }
type chatSendDoneMsg struct{ ref reference.Reference }

func newChatModel(ctx context.Context, sdk *dm.DM, layout *tui.Layout) chatModel {
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

	return chatModel{
		ctx:       ctx,
		sdk:       sdk,
		self:      sdk.SelfPublicKey(),
		peer:      sdk.PeerPublicKey(),
		previews:  make(map[reference.Reference]string),
		textarea:  ta,
		spinner:   s,
		layout:    layout,
		loading:   true,
		atBottom:  true,
		inputRows: 2,
	}
}

func (m chatModel) Init() tea.Cmd {
	return tea.Batch(
		m.fetchMessages(),
		m.spinner.Tick,
		textarea.Blink,
	)
}

func (m chatModel) fetchMessages() tea.Cmd {
	sdk := m.sdk
	ctx := m.ctx
	return func() tea.Msg {
		result, err := sdk.List(ctx, dm.ListOptions{
			Limit:      50,
			Descending: false,
		})
		if err != nil {
			return errMsg{err: err}
		}
		return chatMessagesLoadedMsg{messages: result.Messages}
	}
}

func (m chatModel) startSubscription() tea.Cmd {
	sdk := m.sdk
	ctx := m.ctx
	return func() tea.Msg {
		msgs, errs, err := sdk.Subscribe(ctx, dm.ListOptions{})
		if err != nil {
			return chatSubErrorMsg{err: err}
		}
		return chatSubStartedMsg{msgs: msgs, errs: errs}
	}
}

func (m chatModel) fetchPreviewCmd(msg dm.Message) tea.Cmd {
	sdk := m.sdk
	ctx := m.ctx
	ref := msg.Ref
	return func() tea.Msg {
		preview, err := sdk.Preview(ctx, msg)
		if err != nil {
			return chatPreviewLoadedMsg{ref: ref, preview: ""}
		}
		return chatPreviewLoadedMsg{ref: ref, preview: preview}
	}
}

func (m chatModel) fetchAllPreviews(messages []dm.Message) tea.Cmd {
	n := len(messages)
	if n > 50 {
		n = 50
	}
	cmds := make([]tea.Cmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = m.fetchPreviewCmd(messages[i])
	}
	return tea.Batch(cmds...)
}

func (m *chatModel) appendMessage(msg dm.Message) {
	for _, existing := range m.messages {
		if existing.Ref == msg.Ref {
			return
		}
	}
	m.messages = append(m.messages, msg)
}

func (m chatModel) vpSize() (int, int) {
	bodyWidth, bodyHeight := 80, 20
	if m.layout != nil {
		bodyWidth, bodyHeight = m.layout.BodySize()
	}
	// Reserve: 1 line separator + inputRows lines for textarea + 1 blank
	vpH := bodyHeight - m.inputRows - 2
	if vpH < 3 {
		vpH = 3
	}
	return bodyWidth, vpH
}

func (m *chatModel) rebuildViewport() {
	vpW, vpH := m.vpSize()
	if !m.ready {
		m.viewport = viewport.New(vpW, vpH)
		m.ready = true
	} else {
		m.viewport.Width = vpW
		m.viewport.Height = vpH
	}
	m.viewport.SetContent(m.renderMessages(vpW))
	if m.atBottom {
		m.viewport.GotoBottom()
	}
}

func (m chatModel) renderMessages(width int) string {
	if len(m.messages) == 0 {
		return tui.PreviewStyle.Render("No messages yet. Start typing below.")
	}

	var b strings.Builder
	for _, msg := range m.messages {
		ts := time.Unix(0, msg.Timestamp)
		timeStr := ts.Format("15:04")
		sender := senderLabel(msg.From, m.self)

		preview := m.previews[msg.Ref]
		if preview == "" {
			preview = "(encrypted)"
		}

		line := fmt.Sprintf("%s [%s] %s", timeStr, sender, preview)
		if len(line) > width-2 {
			line = line[:width-5] + "..."
		}

		senderStyle := tui.PeerStyle
		if msg.From == m.self {
			senderStyle = tui.RefStyle
		}
		b.WriteString(tui.PreviewStyle.Render(timeStr+" ") + senderStyle.Render("["+sender+"]") + " " + preview)
		b.WriteString("\n")
	}
	return b.String()
}

func (m chatModel) update(msg tea.Msg) (chatModel, tea.Cmd) {
	switch msg := msg.(type) {
	case chatMessagesLoadedMsg:
		m.messages = msg.messages
		m.loading = false
		m.rebuildViewport()
		return m, m.fetchAllPreviews(msg.messages)

	case chatPreviewLoadedMsg:
		m.previews[msg.ref] = msg.preview
		m.rebuildViewport()
		return m, nil

	case chatSubStartedMsg:
		m.sub = msg.msgs
		m.subErr = msg.errs
		return m, tea.Batch(
			waitForChatMessage(m.sub),
			waitForChatSubError(m.subErr),
		)

	case chatNewMessageMsg:
		m.appendMessage(msg.msg)
		m.rebuildViewport()
		return m, tea.Batch(
			waitForChatMessage(m.sub),
			m.fetchPreviewCmd(msg.msg),
		)

	case chatSubErrorMsg:
		return m, nil

	case chatSendDoneMsg:
		m.sending = false
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case tea.WindowSizeMsg:
		if m.layout != nil {
			w, _ := m.layout.BodySize()
			if w > 4 {
				m.textarea.SetWidth(w - 4)
			}
		}
		m.rebuildViewport()

	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			return m, func() tea.Msg { return backToThreadsMsg{} }
		case "enter":
			text := strings.TrimSpace(m.textarea.Value())
			if text == "" || m.sending {
				return m, nil
			}
			m.sending = true
			m.textarea.Reset()
			sdk := m.sdk
			ctx := m.ctx
			return m, func() tea.Msg {
				result, err := sdk.Send(ctx, []byte(text), nil)
				if err != nil {
					return errMsg{err: err}
				}
				return chatSendDoneMsg{ref: result.Ref}
			}
		}
	}

	// Update textarea
	var taCmd tea.Cmd
	m.textarea, taCmd = m.textarea.Update(msg)

	return m, taCmd
}

func (m chatModel) viewContent() (string, string) {
	helpText := "enter: send • esc: back • scroll: ↑/↓"

	var b strings.Builder

	if m.loading {
		b.WriteString(fmt.Sprintf("%s Loading messages...\n", m.spinner.View()))
		return b.String(), helpText
	}

	// Viewport (messages)
	if m.ready {
		b.WriteString(m.viewport.View())
	}

	// Separator
	bodyWidth := 80
	if m.layout != nil {
		bodyWidth, _ = m.layout.BodySize()
	}
	b.WriteString(tui.PreviewStyle.Render(strings.Repeat("─", bodyWidth)))
	b.WriteString("\n")

	// Textarea
	if m.sending {
		b.WriteString(tui.PreviewStyle.Render("Sending..."))
	} else {
		b.WriteString(m.textarea.View())
	}
	b.WriteString("\n")

	return b.String(), helpText
}

func waitForChatMessage(ch <-chan *dm.Message) tea.Cmd {
	return func() tea.Msg {
		e, ok := <-ch
		if !ok {
			return chatSubErrorMsg{err: fmt.Errorf("subscription closed")}
		}
		return chatNewMessageMsg{msg: *e}
	}
}

func waitForChatSubError(ch <-chan error) tea.Cmd {
	return func() tea.Msg {
		err, ok := <-ch
		if !ok {
			return nil
		}
		return chatSubErrorMsg{err: err}
	}
}

func peerShortHex(pub identity.PublicKey) string {
	return hex.EncodeToString(pub[:4])
}
