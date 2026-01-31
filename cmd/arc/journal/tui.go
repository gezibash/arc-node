package journal

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/pkg/reference"
)

type tuiView int

const (
	viewList tuiView = iota
	viewRead
	viewWrite
)

type journalApp struct {
	ctx    context.Context
	sdk    *journal.Journal
	layout *tui.Layout

	// Centralized domain state.
	entries   []journal.Entry
	previews  map[reference.Reference]string
	connected bool

	// View state.
	view  tuiView
	list  listView
	read  readView
	write writeModel
}

func runTUI(ctx context.Context, c *client.Client, sdk *journal.Journal) error {
	base := tui.NewBase(ctx, c, "journal", "")

	app := &journalApp{
		ctx:      ctx,
		sdk:      sdk,
		view:     viewList,
		list:     newListView(),
		layout:   base.Layout,
		previews: make(map[reference.Reference]string),
	}

	base = base.WithApp(app)
	return base.Run()
}

func (a *journalApp) CanQuit() bool {
	return a.view == viewList
}

func (a *journalApp) Subscribe() tui.SubscribeFunc {
	sdk := a.sdk
	return func(ctx context.Context) (<-chan tea.Msg, error) {
		entries, errs, err := sdk.Subscribe(ctx, journal.ListOptions{
			Descending: true,
		})
		if err != nil {
			return nil, err
		}
		out := make(chan tea.Msg, 64)
		go func() {
			defer close(out)
			for {
				select {
				case e, ok := <-entries:
					if !ok {
						return
					}
					out <- newEntryMsg{entry: *e}
				case _, ok := <-errs:
					if !ok {
						return
					}
					return
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	}
}

func (a *journalApp) Init() tea.Cmd {
	return tea.Batch(
		a.fetchEntries(),
		a.list.spinner.Tick,
	)
}

func (a *journalApp) fetchEntries() tea.Cmd {
	sdk := a.sdk
	ctx := a.ctx
	return func() tea.Msg {
		result, err := sdk.List(ctx, journal.ListOptions{
			Limit:      50,
			Descending: true,
		})
		if err != nil {
			return errMsg{err: err}
		}
		return entriesLoadedMsg{entries: result.Entries}
	}
}

func (a *journalApp) fetchPreviewCmd(e journal.Entry) tea.Cmd {
	sdk := a.sdk
	ctx := a.ctx
	ref := e.Ref
	return func() tea.Msg {
		preview, err := sdk.Preview(ctx, e)
		if err != nil {
			return previewLoadedMsg{ref: ref, preview: ""}
		}
		return previewLoadedMsg{ref: ref, preview: preview}
	}
}

func (a *journalApp) fetchAllPreviews(entries []journal.Entry) tea.Cmd {
	n := 20
	if len(entries) < n {
		n = len(entries)
	}
	cmds := make([]tea.Cmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = a.fetchPreviewCmd(entries[i])
	}
	return tea.Batch(cmds...)
}

func (a *journalApp) startReadContent(entry journal.Entry) tea.Cmd {
	sdk := a.sdk
	ctx := a.ctx
	width := 80
	if a.layout != nil {
		width = a.layout.Width
	}
	return tea.Batch(
		a.read.spinner.Tick,
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
			text = tui.RenderContent(text, width)
			return contentLoadedMsg{content: text}
		},
	)
}

func (a *journalApp) prependEntry(e journal.Entry) {
	for _, existing := range a.entries {
		if existing.Ref == e.Ref {
			return
		}
	}
	a.entries = append([]journal.Entry{e}, a.entries...)
	if a.list.cursor > 0 {
		a.list.cursor++
	}
}

func (a *journalApp) Update(msg tea.Msg) (tui.App, tea.Cmd) {
	switch msg := msg.(type) {
	case tui.SubConnectedMsg:
		a.connected = true
		return a, nil
	case tui.SubClosedMsg:
		a.connected = false
		return a, nil
	// Domain state mutations.
	case entriesLoadedMsg:
		a.entries = msg.entries
		a.list.loading = false
		a.list.cursor = 0
		return a, a.fetchAllPreviews(msg.entries)

	case previewLoadedMsg:
		a.previews[msg.ref] = msg.preview
		return a, nil

	case newEntryMsg:
		a.prependEntry(msg.entry)
		return a, a.fetchPreviewCmd(msg.entry)

	// Navigation.
	case openEntryMsg:
		a.view = viewRead
		a.read = newReadView(msg.entry, a.layout)
		return a, a.startReadContent(msg.entry)
	case contentLoadedMsg:
		a.read.setContent(msg.content)
		return a, nil
	case openWriteMsg:
		a.view = viewWrite
		a.write = newWriteModel(a.ctx, a.sdk, a.layout)
		return a, a.write.Init()
	case writeCompleteMsg:
		a.view = viewList
		a.list.loading = true
		return a, a.fetchEntries()
	case backToListMsg:
		a.view = viewList
		a.list.loading = false
		return a, nil
	case errMsg:
		return a, func() tea.Msg { return tui.ErrMsg{Err: msg.err} }
	case tui.PingResultMsg:
		if msg.Err == nil {
			a.connected = true
		}
	case refreshEntriesMsg:
		a.list.loading = true
		return a, tea.Batch(a.fetchEntries(), a.list.spinner.Tick)
	}

	switch a.view {
	case viewList:
		var cmd tea.Cmd
		a.list, cmd = a.list.update(msg, a.entries)
		return a, cmd
	case viewRead:
		var cmd tea.Cmd
		a.read, cmd = a.read.update(msg)
		return a, cmd
	case viewWrite:
		var cmd tea.Cmd
		a.write, cmd = a.write.update(msg)
		return a, cmd
	}
	return a, nil
}

func (a *journalApp) View() (string, string) {
	switch a.view {
	case viewList:
		return a.list.viewContent(a.entries, a.previews, a.layout)
	case viewRead:
		return a.read.viewContent()
	case viewWrite:
		return a.write.viewContent()
	}
	return "", ""
}

// Messages

type refreshEntriesMsg struct{}
type openEntryMsg struct{ entry journal.Entry }
type openWriteMsg struct{}
type writeCompleteMsg struct{ ref reference.Reference }
type backToListMsg struct{}
type errMsg struct{ err error }

type entriesLoadedMsg struct{ entries []journal.Entry }
type previewLoadedMsg struct {
	ref     reference.Reference
	preview string
}
type contentLoadedMsg struct{ content string }
type newEntryMsg struct{ entry journal.Entry }
