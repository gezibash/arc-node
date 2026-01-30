package journal

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/journal"
)

type tuiView int

const (
	viewList tuiView = iota
	viewRead
	viewWrite
)

type tuiModel struct {
	ctx     context.Context
	sdk     *journal.Journal
	view    tuiView
	list    listModel
	read    readModel
	write   writeModel
	layout  tui.Layout
	err     error
	sub     <-chan *journal.Entry
	subErr  <-chan error
	spinner spinner.Model
	ready   bool
}

func runTUI(ctx context.Context, sdk *journal.Journal, nodePub *identity.PublicKey) error {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)

	var nodeKey string
	if nodePub != nil {
		nodeKey = hex.EncodeToString(nodePub[:4])
	}

	m := tuiModel{
		ctx:  ctx,
		sdk:  sdk,
		view: viewList,
		list: newListModel(ctx, sdk),
		layout: tui.Layout{
			AppName: "journal",
			NodeKey: nodeKey,
		},
		spinner: s,
	}
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func (m tuiModel) Init() tea.Cmd {
	return tea.Batch(
		m.list.Init(),
		m.spinner.Tick,
		m.startSubscription(),
	)
}

func (m tuiModel) startSubscription() tea.Cmd {
	sdk := m.sdk
	ctx := m.ctx
	return func() tea.Msg {
		entries, errs, err := sdk.Subscribe(ctx, journal.ListOptions{
			Descending: true,
		})
		if err != nil {
			return subscriptionErrorMsg{err: err}
		}
		return subscriptionStartedMsg{entries: entries, errs: errs}
	}
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		if msg.String() == "q" && m.view == viewList {
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.layout.Width = msg.Width
		m.layout.Height = msg.Height
		m.list.layout = &m.layout
	case subscriptionStartedMsg:
		m.sub = msg.entries
		m.subErr = msg.errs
		m.ready = true
		m.layout.Connected = true
		m.list.connected = true
		return m, tea.Batch(
			waitForEntry(m.sub),
			waitForSubError(m.subErr),
		)
	case subscriptionErrorMsg:
		return m, nil
	case newEntryMsg:
		m.list.prependEntry(msg.entry)
		return m, tea.Batch(
			waitForEntry(m.sub),
			m.list.fetchPreviewCmd(msg.entry),
		)
	case subErrorMsg:
		return m, nil
	case openEntryMsg:
		m.view = viewRead
		m.read = newReadModel(m.ctx, m.sdk, msg.entry, &m.layout)
		return m, m.read.Init()
	case openWriteMsg:
		m.view = viewWrite
		m.write = newWriteModel(m.ctx, m.sdk, &m.layout)
		return m, m.write.Init()
	case writeCompleteMsg:
		m.view = viewList
		m.list.loading = true
		return m, m.list.fetchEntries()
	case backToListMsg:
		m.view = viewList
		return m, nil
	case errMsg:
		m.err = msg.err
		return m, nil
	case spinner.TickMsg:
		m.layout.Frame++
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	switch m.view {
	case viewList:
		var cmd tea.Cmd
		m.list, cmd = m.list.update(msg)
		return m, cmd
	case viewRead:
		var cmd tea.Cmd
		m.read, cmd = m.read.update(msg)
		return m, cmd
	case viewWrite:
		var cmd tea.Cmd
		m.write, cmd = m.write.update(msg)
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
	case viewList:
		body, help := m.list.viewContent()
		return m.layout.Render(body, help)
	case viewRead:
		body, help := m.read.viewContent()
		return m.layout.Render(body, help)
	case viewWrite:
		body, help := m.write.viewContent()
		return m.layout.Render(body, help)
	}
	return ""
}

// Messages

type openEntryMsg struct{ entry journal.Entry }
type openWriteMsg struct{}
type writeCompleteMsg struct{ ref reference.Reference }
type backToListMsg struct{}
type errMsg struct{ err error }

type subscriptionStartedMsg struct {
	entries <-chan *journal.Entry
	errs    <-chan error
}
type subscriptionErrorMsg struct{ err error }
type newEntryMsg struct{ entry journal.Entry }
type subErrorMsg struct{ err error }

func waitForEntry(ch <-chan *journal.Entry) tea.Cmd {
	return func() tea.Msg {
		e, ok := <-ch
		if !ok {
			return subErrorMsg{err: fmt.Errorf("subscription closed")}
		}
		return newEntryMsg{entry: *e}
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
