package tui

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
)

// SubscribeFunc opens a real-time subscription and returns a single channel
// of tea.Msg. The implementation merges domain messages and errors into this
// one channel. Closing the channel (or context cancellation) signals
// disconnection. Base calls this on init and on each reconnect.
type SubscribeFunc func(ctx context.Context) (<-chan tea.Msg, error)

// App is the interface each TUI app implements.
type App interface {
	Init() tea.Cmd
	Update(msg tea.Msg) (App, tea.Cmd)
	View() (body string, help string)
	CanQuit() bool
	// Subscribe returns a function that Base uses to establish and
	// re-establish the real-time subscription. Return nil if the app
	// does not need a subscription.
	Subscribe() SubscribeFunc
}

// SubConnectedMsg is sent to the app when a subscription is established.
type SubConnectedMsg struct{}

// SubClosedMsg is sent to the app when the subscription channel closes.
type SubClosedMsg struct{}

type resubscribeTickMsg struct{}

// PingTickMsg triggers a new ping.
type PingTickMsg struct{}

// PingResultMsg carries the result of a node ping.
type PingResultMsg struct {
	Latency    time.Duration
	ServerTime time.Time
	Err        error
}

// ErrMsg is a generic error message any app can emit.
type ErrMsg struct{ Err error }

// subState holds mutable subscription state shared across Base value copies.
type subState struct {
	cancel context.CancelFunc
}

// Base handles shared TUI concerns: layout, spinner, ping, error display,
// and subscription lifecycle.
type Base struct {
	Ctx     context.Context
	Client  *client.Client
	Layout  *Layout
	Spinner spinner.Model
	Err     error
	app     App
	subFn   SubscribeFunc
	sub     *subState
}

// NewBase creates a Base with standard spinner and layout.
func NewBase(ctx context.Context, c *client.Client, appName string, nodeKey string) Base {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(AccentColor)

	return Base{
		Ctx:    ctx,
		Client: c,
		Layout: &Layout{
			AppName: appName,
			NodeKey: nodeKey,
		},
		Spinner: s,
	}
}

// WithApp sets the app implementation and returns the Base.
func (b Base) WithApp(app App) Base {
	b.app = app
	b.subFn = app.Subscribe()
	b.sub = &subState{}
	return b
}

// Init starts the spinner tick, ping loop, subscription, and delegates to app.Init().
func (b Base) Init() tea.Cmd {
	cmds := []tea.Cmd{
		b.Spinner.Tick,
		b.pingNode(),
	}
	if b.app != nil {
		cmds = append(cmds, b.app.Init())
	}
	if b.subFn != nil {
		cmds = append(cmds, b.connectSubscription())
	}
	return tea.Batch(cmds...)
}

// connectSubscription cancels any previous subscription, then calls subFn
// with a fresh context. On success it starts a single pump goroutine and
// notifies the app via SubConnectedMsg.
func (b Base) connectSubscription() tea.Cmd {
	if b.sub.cancel != nil {
		b.sub.cancel()
	}
	ctx, cancel := context.WithCancel(b.Ctx)
	b.sub.cancel = cancel

	fn := b.subFn
	return func() tea.Msg {
		if b.Client != nil {
			if _, ok := b.Client.NodeKey(); !ok {
				if _, _, err := b.Client.Ping(ctx); err != nil {
					cancel()
					return SubClosedMsg{}
				}
			}
		}
		ch, err := fn(ctx)
		if err != nil {
			cancel()
			return SubClosedMsg{}
		}
		return subStartedMsg{ch: ch}
	}
}

// subStartedMsg is internal — never seen by the app.
type subStartedMsg struct {
	ch <-chan tea.Msg
}

// subMsgMsg wraps a domain message from the subscription channel so Base
// can identify it and re-issue the pump for the next message.
type subMsgMsg struct {
	ch  <-chan tea.Msg
	msg tea.Msg
}

func pump(ch <-chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return SubClosedMsg{}
		}
		return subMsgMsg{ch: ch, msg: msg}
	}
}

func scheduleResubscribe() tea.Cmd {
	return tea.Tick(3*time.Second, func(_ time.Time) tea.Msg {
		return resubscribeTickMsg{}
	})
}

func (b Base) pingNode() tea.Cmd {
	c := b.Client
	ctx := b.Ctx
	return func() tea.Msg {
		latency, serverTime, err := c.Ping(ctx)
		if err != nil {
			return PingResultMsg{Err: err}
		}
		return PingResultMsg{Latency: latency, ServerTime: serverTime}
	}
}

func schedulePing() tea.Cmd {
	return tea.Tick(5*time.Second, func(_ time.Time) tea.Msg {
		return PingTickMsg{}
	})
}

// Update handles shared messages and delegates the rest to the app.
func (b Base) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return b, tea.Quit
		}
		if msg.String() == "esc" && b.app != nil && b.app.CanQuit() {
			return b, tea.Quit
		}
	case tea.WindowSizeMsg:
		b.Layout.Width = msg.Width
		b.Layout.Height = msg.Height
	case PingResultMsg:
		if msg.Err == nil {
			b.Layout.Latency = msg.Latency
			b.Layout.Connected = true
			if b.Client != nil {
				if info, ok := b.Client.NodeInfo(); ok {
					b.Layout.NodeKey = shortNodeKey(info.PublicKey)
				}
			}
		}
		return b, schedulePing()
	case PingTickMsg:
		return b, b.pingNode()
	case ErrMsg:
		b.Err = msg.Err
		return b, nil
	case spinner.TickMsg:
		b.Layout.Frame++
		b.Spinner, _ = b.Spinner.Update(msg)
		// Don't return — fall through so view spinners animate too.

	// Subscription lifecycle.
	case subStartedMsg:
		cmds := []tea.Cmd{pump(msg.ch)}
		if b.app != nil {
			var appCmd tea.Cmd
			b.app, appCmd = b.app.Update(SubConnectedMsg{})
			if appCmd != nil {
				cmds = append(cmds, appCmd)
			}
		}
		return b, tea.Batch(cmds...)
	case subMsgMsg:
		// Re-issue pump for the next message, then deliver inner msg to app.
		cmds := []tea.Cmd{pump(msg.ch)}
		if b.app != nil {
			var appCmd tea.Cmd
			b.app, appCmd = b.app.Update(msg.msg)
			if appCmd != nil {
				cmds = append(cmds, appCmd)
			}
		}
		return b, tea.Batch(cmds...)
	case SubClosedMsg:
		cmds := []tea.Cmd{scheduleResubscribe()}
		if b.app != nil {
			var appCmd tea.Cmd
			b.app, appCmd = b.app.Update(msg)
			if appCmd != nil {
				cmds = append(cmds, appCmd)
			}
		}
		return b, tea.Batch(cmds...)
	case resubscribeTickMsg:
		if b.subFn != nil {
			return b, b.connectSubscription()
		}
		return b, nil
	}

	if b.app != nil {
		var cmd tea.Cmd
		b.app, cmd = b.app.Update(msg)
		return b, cmd
	}
	return b, nil
}

// View renders the layout frame around the app's view.
func (b Base) View() string {
	if b.Err != nil {
		body := ErrorStyle.Render("Error: "+b.Err.Error()) + "\n\nPress esc to quit.\n"
		return b.Layout.Render(body, "esc: quit")
	}
	if b.app != nil {
		body, help := b.app.View()
		return b.Layout.Render(body, help)
	}
	return b.Layout.Render("", "")
}

// Run creates a tea.Program and runs it.
func (b Base) Run() error {
	p := tea.NewProgram(b, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func shortNodeKey(pub identity.PublicKey) string {
	if pub == (identity.PublicKey{}) {
		return ""
	}
	return hex.EncodeToString(pub[:4])
}
