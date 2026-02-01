package journal

import (
	"context"
	"fmt"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/v2/pkg/reference"
)

type tuiTab int

const (
	tabEntries tuiTab = iota
	tabSync
)

type tuiView int

const (
	viewList tuiView = iota
	viewRead
	viewWrite
)

type journalApp struct {
	ctx       context.Context
	sdk       *journal.Journal
	layout    *tui.Layout
	searchDir string

	// Tab state.
	tab tuiTab

	// Centralized domain state.
	entries    []journal.Entry
	previews   map[reference.Reference]string // keyed on EntryRef
	connected  bool
	hasMore    bool
	nextCursor string

	// Search state.
	searchQuery    string
	searchResults  []journal.SearchResult
	searchSnippets map[reference.Reference]string // keyed on EntryRef
	allEntries     []journal.Entry                // full list preserved during search filtering

	// View state.
	view  tuiView
	list  listView
	read  readView
	write writeModel
	sync  syncView
}

func runTUI(ctx context.Context, c *client.Client, sdk *journal.Journal, searchDir string) error {
	base := tui.NewBase(ctx, c, "journal", "")

	sv := newSyncView(searchDir)
	// Seed local hash from current search index.
	if idx := sdk.SearchIndex(); idx != nil {
		h, _ := idx.ContentHash()
		sv.localHash = reference.Hex(h)
		var n int
		_ = idx.Count(&n)
		sv.localCount = n
		ts, _ := idx.LastIndexedTimestamp()
		sv.localLastUpdate = ts
		// Don't seed remoteHash from persisted state — let a real
		// fetch populate it so we never show stale/wrong-key data.
	}

	app := &journalApp{
		ctx:       ctx,
		sdk:       sdk,
		searchDir: searchDir,
		tab:       tabEntries,
		view:      viewList,
		list:      newListView(),
		sync:      sv,
		layout:    base.Layout,
		previews:  make(map[reference.Reference]string),
	}

	base = base.WithApp(app)
	return base.Run()
}

var (
	activeTabStyle = lipgloss.NewStyle().
			Foreground(tui.AccentColor).
			Bold(true).
			Underline(true)
	inactiveTabStyle = lipgloss.NewStyle().
				Foreground(tui.DimColor)
)

func renderTabBar(active tuiTab) string {
	tabs := []struct {
		label string
		tab   tuiTab
	}{
		{"entries", tabEntries},
		{"sync", tabSync},
	}
	var parts []string
	for _, t := range tabs {
		if t.tab == active {
			parts = append(parts, activeTabStyle.Render(t.label))
		} else {
			parts = append(parts, inactiveTabStyle.Render(t.label))
		}
	}
	return "  " + lipgloss.JoinHorizontal(lipgloss.Top, joinWith(parts, "  ")) + "\n\n"
}

func joinWith(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}

func (a *journalApp) CanQuit() bool {
	if a.view != viewList && a.tab != tabSync {
		return false
	}
	// Don't quit while searching or viewing search results.
	if a.list.searching || a.searchQuery != "" {
		return false
	}
	return true
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

const pageSize = 5

func (a *journalApp) fetchEntries() tea.Cmd {
	sdk := a.sdk
	ctx := a.ctx
	return func() tea.Msg {
		result, err := sdk.List(ctx, journal.ListOptions{
			Limit:      pageSize,
			Descending: true,
		})
		if err != nil {
			return errMsg{err: err}
		}
		return entriesLoadedMsg{
			entries:    result.Entries,
			hasMore:    result.HasMore,
			nextCursor: result.NextCursor,
		}
	}
}

func (a *journalApp) fetchMoreEntries() tea.Cmd {
	sdk := a.sdk
	ctx := a.ctx
	cursor := a.nextCursor
	return func() tea.Msg {
		result, err := sdk.List(ctx, journal.ListOptions{
			Limit:      pageSize,
			Cursor:     cursor,
			Descending: true,
		})
		if err != nil {
			return errMsg{err: err}
		}
		return moreEntriesLoadedMsg{
			entries:    result.Entries,
			hasMore:    result.HasMore,
			nextCursor: result.NextCursor,
		}
	}
}

func (a *journalApp) fetchPreviewCmd(e journal.Entry) tea.Cmd {
	sdk := a.sdk
	ctx := a.ctx
	entryRef := e.EntryRef
	return func() tea.Msg {
		preview, err := sdk.Preview(ctx, e)
		if err != nil {
			return previewLoadedMsg{ref: entryRef, preview: ""}
		}
		return previewLoadedMsg{ref: entryRef, preview: preview}
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
			// If labels are missing (e.g. from a search result), look up the
			// entry by its deterministic entryRef label. This is a label
			// equality filter pushed to the backend, so Limit 1 works.
			if contentHex == "" {
				var zeroRef reference.Reference
				if entry.EntryRef != zeroRef {
					result, err := sdk.List(ctx, journal.ListOptions{
						Labels: map[string]string{"entry": reference.Hex(entry.EntryRef)},
						Limit:  1,
					})
					if err != nil {
						return errMsg{err: fmt.Errorf("lookup entry: %w", err)}
					}
					if len(result.Entries) > 0 {
						contentHex = result.Entries[0].Labels["content"]
					}
				}
				if contentHex == "" {
					return errMsg{err: fmt.Errorf("entry not found")}
				}
			}
			ref, err := reference.FromHex(contentHex)
			if err != nil {
				return errMsg{err: err}
			}
			readResult, err := sdk.Read(ctx, ref)
			if err != nil {
				return errMsg{err: err}
			}
			text := string(readResult.Content)
			text = tui.RenderContent(text, width)
			return contentLoadedMsg{content: text}
		},
	)
}

func (a *journalApp) prependEntry(e journal.Entry) {
	var zeroRef reference.Reference
	for _, existing := range a.entries {
		if existing.Ref == e.Ref {
			return
		}
		if e.EntryRef != zeroRef && existing.EntryRef == e.EntryRef {
			return
		}
	}
	a.entries = append([]journal.Entry{e}, a.entries...)
}

func (a *journalApp) rebuildListItems() {
	items := make([]list.Item, len(a.entries))
	for i, e := range a.entries {
		preview := a.previews[e.EntryRef]
		if snip, ok := a.searchSnippets[e.EntryRef]; ok && snip != "" {
			preview = snip
		}
		items[i] = entryItem{entry: e, preview: preview}
	}
	a.list.setItems(items)
}

func (a *journalApp) Update(msg tea.Msg) (tui.App, tea.Cmd) {
	// Tab switching — only when on the list view (not in read/write/search-input).
	if km, ok := msg.(tea.KeyMsg); ok && km.String() == "tab" {
		if a.tab == tabEntries && a.view == viewList && !a.list.searching {
			a.tab = tabSync
			// Refresh local stats from the current index.
			if idx := a.sdk.SearchIndex(); idx != nil {
				h, _ := idx.ContentHash()
				a.sync.localHash = reference.Hex(h)
				var n int
				_ = idx.Count(&n)
				a.sync.localCount = n
				ts, _ := idx.LastIndexedTimestamp()
				a.sync.localLastUpdate = ts
			}
			// Auto-fetch remote info if we haven't yet.
			if a.sync.remoteLastUpdate == 0 && a.sync.status == syncIdle {
				a.sync.status = syncFetching
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, fetchSyncCmd(a.ctx, a.sdk))
			}
			return a, nil
		} else if a.tab == tabSync && a.sync.status == syncIdle {
			a.tab = tabEntries
			return a, nil
		}
	}

	// Sync view messages.
	switch msg.(type) {
	case syncFetchedMsg, syncPulledMsg, syncPushedMsg, syncReindexedMsg:
		var cmd tea.Cmd
		a.sync, cmd = a.sync.update(msg)
		return a, cmd
	}

	// Dispatch sync key events when on sync tab.
	if a.tab == tabSync {
		if km, ok := msg.(tea.KeyMsg); ok {
			// Any non-confirm key press resets the confirm state.
			prevConfirm := a.sync.confirm
			if km.String() != "p" && km.String() != "P" {
				a.sync.confirm = confirmNone
			}
			switch km.String() {
			case "f":
				a.sync.status = syncFetching
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, fetchSyncCmd(a.ctx, a.sdk))
			case "p":
				if a.sync.localHash == a.sync.remoteHash && a.sync.remoteHash != "" {
					a.sync.message = "already up to date"
					a.sync.confirm = confirmNone
					return a, nil
				}
				// Warn if local is newer than remote.
				if prevConfirm != confirmPull &&
					a.sync.localLastUpdate > 0 && a.sync.remoteLastUpdate > 0 &&
					a.sync.localLastUpdate > a.sync.remoteLastUpdate {
					a.sync.confirm = confirmPull
					a.sync.message = fmt.Sprintf("local is newer (%d vs %d entries) — press p again to confirm",
						a.sync.localCount, a.sync.remoteCount)
					return a, nil
				}
				a.sync.confirm = confirmNone
				a.sync.status = syncPulling
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, pullSyncCmd(a.ctx, a.sdk, a.searchDir+"/search.db"))
			case "P":
				if a.sync.localHash == a.sync.remoteHash && a.sync.remoteHash != "" {
					a.sync.message = "everything up to date"
					a.sync.confirm = confirmNone
					return a, nil
				}
				// Warn if remote is newer than local.
				if prevConfirm != confirmPush &&
					a.sync.localLastUpdate > 0 && a.sync.remoteLastUpdate > 0 &&
					a.sync.remoteLastUpdate > a.sync.localLastUpdate {
					a.sync.confirm = confirmPush
					a.sync.message = fmt.Sprintf("remote is newer (%d vs %d entries) — press P again to confirm",
						a.sync.remoteCount, a.sync.localCount)
					return a, nil
				}
				a.sync.confirm = confirmNone
				a.sync.status = syncPushing
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, pushSyncCmd(a.ctx, a.sdk))
			case "r":
				a.sync.status = syncReindexing
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, reindexSyncCmd(a.ctx, a.sdk))
			}
		}
		var cmd tea.Cmd
		a.sync, cmd = a.sync.update(msg)
		return a, cmd
	}

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
		a.hasMore = msg.hasMore
		a.nextCursor = msg.nextCursor
		a.list.loading = false
		a.rebuildListItems()
		a.list.model.Select(0)
		return a, a.fetchAllPreviews(msg.entries)

	case moreEntriesLoadedMsg:
		added := make([]journal.Entry, 0, len(msg.entries))
		if len(msg.entries) > 0 {
			seen := make(map[reference.Reference]bool, len(a.entries))
			for _, e := range a.entries {
				seen[e.Ref] = true
			}
			for _, e := range msg.entries {
				if seen[e.Ref] {
					continue
				}
				seen[e.Ref] = true
				a.entries = append(a.entries, e)
				added = append(added, e)
			}
		}
		a.hasMore = msg.hasMore
		a.nextCursor = msg.nextCursor
		a.list.loading = false
		a.rebuildListItems()
		return a, a.fetchAllPreviews(added)

	case loadMoreMsg:
		if a.hasMore {
			a.list.loading = true
			return a, tea.Batch(a.fetchMoreEntries(), a.list.spinner.Tick)
		}
		a.list.loading = false
		return a, nil

	case previewLoadedMsg:
		a.previews[msg.ref] = msg.preview
		a.rebuildListItems()
		return a, nil

	case newEntryMsg:
		a.prependEntry(msg.entry)
		if a.allEntries != nil {
			a.allEntries = append([]journal.Entry{msg.entry}, a.allEntries...)
		}
		a.rebuildListItems()
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
	case executeSearchMsg:
		sdk := a.sdk
		query := msg.query
		return a, func() tea.Msg {
			resp, err := sdk.Search(context.Background(), query, journal.SearchOptions{})
			if err != nil {
				return searchResultsMsg{query: query, results: nil}
			}
			return searchResultsMsg{query: query, results: resp.Results}
		}
	case searchResultsMsg:
		a.searchQuery = msg.query
		a.searchResults = msg.results
		a.searchSnippets = make(map[reference.Reference]string)
		if a.allEntries == nil {
			a.allEntries = a.entries
		}
		// Index known entries by entryRef for fast lookup.
		known := make(map[reference.Reference]journal.Entry, len(a.allEntries))
		for _, e := range a.allEntries {
			var zeroRef reference.Reference
			if e.EntryRef != zeroRef {
				known[e.EntryRef] = e
			}
		}
		// Build entries from search results, deduplicating by entryRef.
		seen := make(map[reference.Reference]bool, len(msg.results))
		var entries []journal.Entry
		for _, r := range msg.results {
			if seen[r.Ref] {
				continue
			}
			seen[r.Ref] = true
			a.searchSnippets[r.Ref] = r.Snippet // r.Ref is now entryRef
			if e, ok := known[r.Ref]; ok {
				entries = append(entries, e)
			} else {
				entries = append(entries, journal.Entry{EntryRef: r.Ref, Timestamp: r.Timestamp})
			}
		}
		a.entries = entries
		a.hasMore = false
		a.rebuildListItems()
		a.list.model.Select(0)
		return a, nil
	case clearSearchMsg:
		a.searchQuery = ""
		a.searchResults = nil
		a.searchSnippets = nil
		if a.allEntries != nil {
			a.entries = a.allEntries
			a.allEntries = nil
		}
		a.rebuildListItems()
		a.list.model.Select(0)
		return a, nil
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
	// Sub-views (read/write) don't show the tab bar.
	switch a.view {
	case viewRead:
		return a.read.viewContent()
	case viewWrite:
		return a.write.viewContent()
	}

	tabBar := renderTabBar(a.tab)

	switch a.tab {
	case tabSync:
		body, help := a.sync.viewContent(a.layout)
		return tabBar + body, help
	default:
		body, help := a.list.viewContent(a.entries, a.previews, a.searchSnippets, a.searchQuery, a.hasMore, a.layout)
		return tabBar + body, help
	}
}

// Messages

type refreshEntriesMsg struct{}
type openEntryMsg struct{ entry journal.Entry }
type openWriteMsg struct{}
type writeCompleteMsg struct{ ref reference.Reference }
type backToListMsg struct{}
type errMsg struct{ err error }

type entriesLoadedMsg struct {
	entries    []journal.Entry
	hasMore    bool
	nextCursor string
}
type moreEntriesLoadedMsg struct {
	entries    []journal.Entry
	hasMore    bool
	nextCursor string
}
type previewLoadedMsg struct {
	ref     reference.Reference
	preview string
}
type contentLoadedMsg struct{ content string }
type newEntryMsg struct{ entry journal.Entry }
type loadMoreMsg struct{}
type executeSearchMsg struct{ query string }
type clearSearchMsg struct{}
type searchResultsMsg struct {
	query   string
	results []journal.SearchResult
}
