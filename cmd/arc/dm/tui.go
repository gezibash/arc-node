package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/dm"
)

type tuiView int

const (
	viewThreads tuiView = iota
	viewChat
	viewRead
)

type tuiModel struct {
	ctx     context.Context
	client  *client.Client
	threads *dm.Threads
	self    identity.PublicKey
	view    tuiView
	thList  threadsModel
	chat    chatModel
	read    readModel
	layout  tui.Layout
	err     error
	sub     <-chan *dm.Message
	subErr  <-chan error
	spinner spinner.Model
	ready   bool
	nodePub *identity.PublicKey
}

func runTUI(ctx context.Context, c *client.Client, threads *dm.Threads, kp *identity.Keypair, nodePub *identity.PublicKey) error {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)

	var nodeKey string
	if nodePub != nil {
		nodeKey = hex.EncodeToString(nodePub[:4])
	}

	self := kp.PublicKey()

	m := tuiModel{
		ctx:     ctx,
		client:  c,
		threads: threads,
		self:    self,
		view:    viewThreads,
		thList:  newThreadsModel(ctx, threads, self),
		layout: tui.Layout{
			AppName: "dm",
			NodeKey: nodeKey,
		},
		spinner: s,
		nodePub: nodePub,
	}
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func (m tuiModel) Init() tea.Cmd {
	return tea.Batch(
		m.thList.Init(),
		m.spinner.Tick,
		m.startGlobalSubscription(),
		m.pingNode(),
	)
}

func (m tuiModel) pingNode() tea.Cmd {
	c := m.client
	ctx := m.ctx
	return func() tea.Msg {
		latency, err := c.Ping(ctx)
		if err != nil {
			return pingResultMsg{err: err}
		}
		return pingResultMsg{latency: latency}
	}
}

func schedulePing() tea.Cmd {
	return tea.Tick(5*time.Second, func(_ time.Time) tea.Msg {
		return pingTickMsg{}
	})
}

func (m tuiModel) startGlobalSubscription() tea.Cmd {
	threads := m.threads
	ctx := m.ctx
	return func() tea.Msg {
		msgs, errs, err := threads.SubscribeAll(ctx)
		if err != nil {
			return subscriptionErrorMsg{err: err}
		}
		return subscriptionStartedMsg{msgs: msgs, errs: errs}
	}
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		if msg.String() == "q" && m.view == viewThreads && !m.thList.prompting {
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.layout.Width = msg.Width
		m.layout.Height = msg.Height
		m.thList.layout = &m.layout
	case subscriptionStartedMsg:
		m.sub = msg.msgs
		m.subErr = msg.errs
		m.ready = true
		return m, tea.Batch(
			waitForMessage(m.sub),
			waitForSubError(m.subErr),
		)
	case subscriptionErrorMsg:
		m.err = msg.err
		return m, nil
	case newMessageMsg:
		// Update thread list regardless of current view.
		m.thList.updateThread(msg.msg)
		var cmds []tea.Cmd
		cmds = append(cmds, waitForMessage(m.sub))
		// If we're in the chat view for this conversation, forward to chat.
		if m.view == viewChat {
			convID := msg.msg.Labels["conversation"]
			chatPeer := m.chat.peer
			expectedConv := dm.ConversationID(m.self, chatPeer)
			if convID == expectedConv {
				m.chat.appendMessage(msg.msg)
				m.chat.rebuildViewport()
				cmds = append(cmds, m.chat.fetchPreviewCmd(msg.msg))
			}
		}
		return m, tea.Batch(cmds...)
	case subErrorMsg:
		m.err = msg.err
		return m, nil
	case openThreadMsg:
		sdk, err := m.threads.OpenConversation(msg.thread.PeerPub)
		if err != nil {
			m.err = err
			return m, nil
		}
		m.view = viewChat
		m.layout.AppName = "dm · " + hex.EncodeToString(msg.thread.PeerPub[:4])
		m.chat = newChatModel(m.ctx, sdk, &m.layout)
		return m, m.chat.Init()
	case backToThreadsMsg:
		m.view = viewThreads
		m.layout.AppName = "dm"
		return m, nil
	case openMessageMsg:
		m.view = viewRead
		m.read = newReadModel(m.ctx, m.chat.sdk, msg.msg, &m.layout)
		return m, m.read.Init()
	case backToChatMsg:
		m.view = viewChat
		return m, nil
	case errMsg:
		m.err = msg.err
		return m, nil
	case pingResultMsg:
		if msg.err == nil {
			m.layout.Latency = msg.latency
			m.layout.Connected = true
			m.thList.connected = true
		}
		return m, schedulePing()
	case pingTickMsg:
		return m, m.pingNode()
	case spinner.TickMsg:
		m.layout.Frame++
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	switch m.view {
	case viewThreads:
		var cmd tea.Cmd
		m.thList, cmd = m.thList.update(msg)
		return m, cmd
	case viewChat:
		var cmd tea.Cmd
		m.chat, cmd = m.chat.update(msg)
		return m, cmd
	case viewRead:
		var cmd tea.Cmd
		m.read, cmd = m.read.update(msg)
		return m, cmd
	}
	return m, nil
}

func (m tuiModel) View() string {
	if m.err != nil {
		body := tui.ErrorStyle.Render("Error: "+m.err.Error()) + "\n\nPress q to quit.\n"
		return m.layout.Render(body, "q: quit")
	}
	switch m.view {
	case viewThreads:
		body, help := m.thList.viewContent()
		return m.layout.Render(body, help)
	case viewChat:
		body, help := m.chat.viewContent()
		return m.layout.Render(body, help)
	case viewRead:
		body, help := m.read.viewContent()
		return m.layout.Render(body, help)
	}
	return ""
}

// Messages

type openThreadMsg struct{ thread dm.Thread }
type backToThreadsMsg struct{}
type openMessageMsg struct{ msg dm.Message }
type backToChatMsg struct{}
type errMsg struct{ err error }

type subscriptionStartedMsg struct {
	msgs <-chan *dm.Message
	errs <-chan error
}
type subscriptionErrorMsg struct{ err error }
type newMessageMsg struct{ msg dm.Message }
type subErrorMsg struct{ err error }
type pingTickMsg struct{}
type pingResultMsg struct {
	latency time.Duration
	err     error
}

func waitForMessage(ch <-chan *dm.Message) tea.Cmd {
	return func() tea.Msg {
		e, ok := <-ch
		if !ok {
			return subErrorMsg{err: fmt.Errorf("subscription closed")}
		}
		return newMessageMsg{msg: *e}
	}
}

func waitForSubError(ch <-chan error) tea.Cmd {
	return func() tea.Msg {
		err, ok := <-ch
		if !ok {
			return nil
		}
		return subErrorMsg{err: err}
	}
}
