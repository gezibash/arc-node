package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

type tuiTab int

const (
	tabThreads tuiTab = iota
	tabSync
)

type tuiView int

const (
	viewThreads tuiView = iota
	viewChat
	viewRead
)

type dmApp struct {
	ctx       context.Context
	client    *client.Client
	threads   *dm.Threads
	self      identity.PublicKey
	layout    *tui.Layout
	searchDir string

	// Tab state.
	tab tuiTab

	// Centralized domain state.
	threadList []dm.Thread
	chatMsgs   []dm.Message
	previews   map[reference.Reference]string
	unread     map[string]int // convID → unread count
	connected  bool

	// Search state.
	searchQuery string

	// Current chat conversation SDK (nil when on threads view).
	chatSDK *dm.DM

	// View state.
	view   tuiView
	thList threadsView
	chat   chatView
	read   readView
	sync   syncView
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
		{"threads", tabThreads},
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
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "  "
		}
		result += p
	}
	return "  " + result + "\n\n"
}

func runTUI(ctx context.Context, c *client.Client, threads *dm.Threads, kp *identity.Keypair, searchDir string) error {
	self := kp.PublicKey()
	base := tui.NewBase(ctx, c, "dm", "")

	sv := newSyncView(searchDir)
	if idx := threads.SearchIndex(); idx != nil {
		h, _ := idx.ContentHash(context.Background())
		sv.localHash = reference.Hex(h)
		var n int
		_ = idx.Count(context.Background(), &n)
		sv.localCount = n
		ts, _ := idx.LastIndexedTimestamp(context.Background())
		sv.localLastUpdate = ts
	}

	app := &dmApp{
		ctx:       ctx,
		client:    c,
		threads:   threads,
		self:      self,
		layout:    base.Layout,
		searchDir: searchDir,
		tab:       tabThreads,
		previews:  make(map[reference.Reference]string),
		unread:    make(map[string]int),
		view:      viewThreads,
		thList:    newThreadsView(),
		sync:      sv,
	}

	base = base.WithApp(app)
	return base.Run()
}

func (a *dmApp) CanQuit() bool {
	if a.view != viewThreads && a.tab != tabSync {
		return false
	}
	if a.thList.prompting || a.thList.searching || a.searchQuery != "" {
		return false
	}
	return true
}

func (a *dmApp) Subscribe() tui.SubscribeFunc {
	threads := a.threads
	return func(ctx context.Context) (<-chan tea.Msg, error) {
		msgs, errs, err := threads.SubscribeAll(ctx)
		if err != nil {
			return nil, err
		}
		out := make(chan tea.Msg, 64)
		go func() {
			defer close(out)
			for {
				select {
				case m, ok := <-msgs:
					if !ok {
						return
					}
					out <- newMessageMsg{msg: *m}
				case _, ok := <-errs:
					if !ok {
						return
					}
					// Error on the subscription — close to trigger reconnect.
					return
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	}
}

func (a *dmApp) Init() tea.Cmd {
	return tea.Batch(
		a.fetchThreads(),
		a.thList.spinner.Tick,
	)
}

func (a *dmApp) fetchThreads() tea.Cmd {
	threads := a.threads
	ctx := a.ctx
	return func() tea.Msg {
		result, err := threads.ListThreads(ctx)
		if err != nil {
			return errMsg{err: err}
		}
		return threadsLoadedMsg{threads: result}
	}
}

func (a *dmApp) fetchAllThreadPreviews() tea.Cmd {
	n := len(a.threadList)
	if n > 10 {
		n = 10
	}
	cmds := make([]tea.Cmd, n)
	for i := 0; i < n; i++ {
		th := a.threadList[i]
		threads := a.threads
		ctx := a.ctx
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

func (a *dmApp) fetchThreadPreview(msg dm.Message) tea.Cmd {
	threads := a.threads
	ctx := a.ctx
	self := a.self
	convID := msg.Labels["conversation"]
	return func() tea.Msg {
		peer := msg.From
		if peer == self {
			peer = msg.To
		}
		preview, err := threads.PreviewThread(ctx, dm.Thread{
			ConvID:  convID,
			PeerPub: peer,
			LastMsg: msg,
		})
		if err != nil {
			return threadPreviewLoadedMsg{convID: convID, preview: ""}
		}
		return threadPreviewLoadedMsg{convID: convID, preview: preview}
	}
}

func (a *dmApp) fetchChatMessages() tea.Cmd {
	sdk := a.chatSDK
	ctx := a.ctx
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

func (a *dmApp) fetchPreviewCmd(msg dm.Message) tea.Cmd {
	sdk := a.chatSDK
	threads := a.threads
	ctx := a.ctx
	ref := msg.Ref
	return func() tea.Msg {
		contentHex := msg.Labels["content"]
		if contentHex == "" {
			return chatPreviewLoadedMsg{ref: ref, preview: ""}
		}
		contentRef, err := reference.FromHex(contentHex)
		if err != nil {
			return chatPreviewLoadedMsg{ref: ref, preview: ""}
		}
		result, err := sdk.Read(ctx, contentRef)
		if err != nil {
			return chatPreviewLoadedMsg{ref: ref, preview: ""}
		}
		plaintext := string(result.Content)

		// Index for search.
		threads.IndexMessage(ctx, contentRef, ref, plaintext, msg.Timestamp)

		return chatPreviewLoadedMsg{ref: ref, preview: truncateLocalPreview(plaintext)}
	}
}

func (a *dmApp) fetchAllChatPreviews(messages []dm.Message) tea.Cmd {
	n := len(messages)
	if n > 50 {
		n = 50
	}
	cmds := make([]tea.Cmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = a.fetchPreviewCmd(messages[i])
	}
	return tea.Batch(cmds...)
}

func (a *dmApp) updateThreadFromMessage(msg dm.Message) {
	convID := msg.Labels["conversation"]
	if convID == "" {
		return
	}

	peer := msg.From
	if msg.From == a.self {
		peer = msg.To
	}

	for i, th := range a.threadList {
		if th.ConvID == convID {
			a.threadList[i].LastMsg = msg
			a.threadList[i].Preview = ""
			updated := a.threadList[i]
			copy(a.threadList[1:i+1], a.threadList[:i])
			a.threadList[0] = updated
			return
		}
	}

	a.threadList = append([]dm.Thread{{
		ConvID:  convID,
		PeerPub: peer,
		LastMsg: msg,
	}}, a.threadList...)
}

func (a *dmApp) appendChatMessage(msg dm.Message) {
	for _, existing := range a.chatMsgs {
		if existing.Ref == msg.Ref {
			return
		}
	}
	a.chatMsgs = append(a.chatMsgs, msg)
}

func (a *dmApp) Update(msg tea.Msg) (tui.App, tea.Cmd) {
	// Tab switching — only when on threads view, not prompting or searching.
	if km, ok := msg.(tea.KeyMsg); ok && km.String() == "tab" {
		if a.tab == tabThreads && a.view == viewThreads && !a.thList.prompting && a.searchQuery == "" {
			a.tab = tabSync
			a.sync.threadCount = len(a.threadList)
			if idx := a.threads.SearchIndex(); idx != nil {
				h, _ := idx.ContentHash(context.Background())
				a.sync.localHash = reference.Hex(h)
				var n int
				_ = idx.Count(context.Background(), &n)
				a.sync.localCount = n
				ts, _ := idx.LastIndexedTimestamp(context.Background())
				a.sync.localLastUpdate = ts
			}
			if a.sync.status == syncIdle {
				a.sync.status = syncFetching
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, fetchSyncCmd(a.ctx, a.threads))
			}
			return a, nil
		} else if a.tab == tabSync && a.sync.status == syncIdle {
			a.tab = tabThreads
			if a.thList.cursor >= len(a.threadList) {
				a.thList.cursor = max(0, len(a.threadList)-1)
			}
			return a, nil
		}
	}

	// Sync view messages.
	switch msg.(type) {
	case syncFetchedMsg:
		var cmd tea.Cmd
		a.sync, cmd = a.sync.update(msg)
		return a, cmd
	case syncPulledMsg, syncPushedMsg:
		var cmd tea.Cmd
		a.sync, cmd = a.sync.update(msg)
		// Refresh local stats.
		if idx := a.threads.SearchIndex(); idx != nil {
			h, _ := idx.ContentHash(context.Background())
			a.sync.localHash = reference.Hex(h)
			var n int
			_ = idx.Count(context.Background(), &n)
			a.sync.localCount = n
			ts, _ := idx.LastIndexedTimestamp(context.Background())
			a.sync.localLastUpdate = ts
		}
		// Re-fetch remote status after pull/push to keep view current.
		if a.sync.status == syncIdle {
			a.sync.status = syncFetching
			return a, tea.Batch(cmd, a.sync.spinner.Tick, fetchSyncCmd(a.ctx, a.threads))
		}
		return a, cmd
	case syncReindexedMsg:
		var cmd tea.Cmd
		a.sync, cmd = a.sync.update(msg)
		return a, cmd
	}

	// Dispatch sync key events when on sync tab.
	if a.tab == tabSync {
		if km, ok := msg.(tea.KeyMsg); ok {
			prevConfirm := a.sync.confirm
			if km.String() != "p" && km.String() != "P" {
				a.sync.confirm = confirmNone
			}
			switch km.String() {
			case "f":
				a.sync.status = syncFetching
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, fetchSyncCmd(a.ctx, a.threads))
			case "p":
				if a.sync.localHash == a.sync.remoteHash && a.sync.remoteHash != "" {
					a.sync.message = "already up to date"
					a.sync.confirm = confirmNone
					return a, nil
				}
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
				return a, tea.Batch(a.sync.spinner.Tick, pullSyncCmd(a.ctx, a.threads, a.searchDir+"/search.db"))
			case "P":
				if a.sync.localHash == a.sync.remoteHash && a.sync.remoteHash != "" {
					a.sync.message = "everything up to date"
					a.sync.confirm = confirmNone
					return a, nil
				}
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
				return a, tea.Batch(a.sync.spinner.Tick, pushSyncCmd(a.ctx, a.threads))
			case "r":
				a.sync.status = syncReindexing
				a.sync.message = ""
				return a, tea.Batch(a.sync.spinner.Tick, reindexSyncCmd(a.ctx, a.threads))
			}
		}
		var cmd tea.Cmd
		a.sync, cmd = a.sync.update(msg)
		return a, cmd
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		if a.view == viewChat {
			a.chat = onResizeChat(a.chat, a.layout)
			a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
		}

	case tui.SubConnectedMsg:
		a.connected = true
		return a, nil
	case tui.SubClosedMsg:
		a.connected = false
		return a, nil
	// Domain state mutations.
	case threadsLoadedMsg:
		a.threadList = msg.threads
		a.thList.loading = false
		// Compute unread counts from persisted last-read timestamps.
		if idx := a.threads.SearchIndex(); idx != nil {
			lastReads, _ := idx.AllLastRead(context.Background())
			for _, th := range a.threadList {
				lr := lastReads[th.ConvID]
				if th.LastMsg.Timestamp > lr {
					a.unread[th.ConvID] = 1 // at least 1 unread
				}
			}
		}
		return a, a.fetchAllThreadPreviews()

	case threadPreviewLoadedMsg:
		for i, th := range a.threadList {
			if th.ConvID == msg.convID {
				a.threadList[i].Preview = msg.preview
				break
			}
		}
		return a, nil

	case newMessageMsg:
		a.updateThreadFromMessage(msg.msg)
		convID := msg.msg.Labels["conversation"]
		var cmds []tea.Cmd
		// Fetch thread preview for the updated thread.
		cmds = append(cmds, a.fetchThreadPreview(msg.msg))
		// Track unread: if we're viewing this exact conversation, don't increment.
		currentConv := ""
		if a.view == viewChat && a.chatSDK != nil {
			currentConv = dm.ConversationID(a.self, a.chatSDK.PeerPublicKey())
		}
		if convID != currentConv && msg.msg.From != a.self {
			a.unread[convID]++
		}
		if a.view == viewChat && a.chatSDK != nil {
			if convID == currentConv {
				a.appendChatMessage(msg.msg)
				if !a.chat.atBottom {
					a.chat.newMsgCount++
				}
				a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
				cmds = append(cmds, a.fetchPreviewCmd(msg.msg))
			}
		}
		return a, tea.Batch(cmds...)

	case chatMessagesLoadedMsg:
		if a.view != viewChat || a.chatSDK == nil {
			return a, nil
		}
		a.chatMsgs = msg.messages
		a.chat.loading = false
		a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
		return a, a.fetchAllChatPreviews(msg.messages)

	case chatPreviewLoadedMsg:
		a.previews[msg.ref] = msg.preview
		if a.view == viewChat {
			a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
		}
		// Update thread preview if this is the latest message in the thread.
		if msg.preview != "" && len(a.threadList) > 0 {
			for i, th := range a.threadList {
				if th.LastMsg.Ref == msg.ref {
					a.threadList[i].Preview = msg.preview
					break
				}
			}
		}
		return a, nil

	case sendMsg:
		sdk := a.chatSDK
		ctx := a.ctx
		text := msg.text
		return a, func() tea.Msg {
			result, err := sdk.Send(ctx, []byte(text), nil)
			if err != nil {
				return errMsg{err: err}
			}
			return chatSendDoneMsg{ref: result.Ref, text: text, ts: time.Now().UnixMilli()}
		}
	case chatSendDoneMsg:
		a.chat.sending = false
		if a.view == viewChat && a.chatSDK != nil {
			peer := a.chatSDK.PeerPublicKey()
			convID := dm.ConversationID(a.self, peer)
			localMsg := dm.Message{
				Ref:       msg.ref,
				Labels:    localConversationLabels(convID, a.self, peer),
				Timestamp: msg.ts,
				From:      a.self,
				To:        peer,
			}
			a.updateThreadFromMessage(localMsg)
			a.appendChatMessage(localMsg)
			if msg.text != "" {
				preview := truncateLocalPreview(msg.text)
				a.previews[msg.ref] = preview
				// Update thread preview so threads list shows decrypted text.
				for i, th := range a.threadList {
					if th.ConvID == convID {
						a.threadList[i].Preview = preview
						break
					}
				}
			}
			a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
		}
		return a, nil

	// Navigation.
	case openThreadMsg:
		sdk, err := a.threads.OpenConversation(msg.thread.PeerPub)
		if err != nil {
			return a, func() tea.Msg { return tui.ErrMsg{Err: err} }
		}
		convID := dm.ConversationID(a.self, msg.thread.PeerPub)
		a.view = viewChat
		a.chatSDK = sdk
		a.chatMsgs = nil
		a.previews = make(map[reference.Reference]string)
		// Clear unread and persist last-read timestamp.
		delete(a.unread, convID)
		if idx := a.threads.SearchIndex(); idx != nil && msg.thread.LastMsg.Timestamp > 0 {
			_ = idx.MarkRead(context.Background(), convID, msg.thread.LastMsg.Timestamp)
		}
		a.layout.AppName = "dm · " + hex.EncodeToString(msg.thread.PeerPub[:4])
		a.chat = newChatView(a.layout)
		return a, tea.Batch(
			a.fetchChatMessages(),
			a.chat.spinner.Tick,
			a.chat.textareaBlink(),
		)
	case backToThreadsMsg:
		// Persist last-read for the conversation we're leaving.
		if a.chatSDK != nil && len(a.chatMsgs) > 0 {
			convID := dm.ConversationID(a.self, a.chatSDK.PeerPublicKey())
			lastTS := a.chatMsgs[len(a.chatMsgs)-1].Timestamp
			delete(a.unread, convID)
			if idx := a.threads.SearchIndex(); idx != nil {
				_ = idx.MarkRead(context.Background(), convID, lastTS)
			}
		}
		a.view = viewThreads
		a.chatSDK = nil
		a.chatMsgs = nil
		a.thList.loading = false
		a.layout.AppName = "dm"
		return a, nil
	case openMessageMsg:
		a.view = viewRead
		a.read = newReadView(msg.msg, a.layout)
		return a, a.startReadContent(msg.msg)
	case backToChatMsg:
		a.view = viewChat
		return a, nil
	case contentLoadedMsg:
		a.read.setContent(msg.content)
		return a, nil
	case executeSearchMsg:
		threads := a.threads
		ctx := a.ctx
		query := msg.query
		a.searchQuery = query
		return a, func() tea.Msg {
			resp, err := threads.Search(ctx, query, dm.SearchOptions{Limit: 50})
			if err != nil {
				return searchResultsMsg{query: query, results: nil}
			}
			return searchResultsMsg{query: query, results: resp.Results}
		}
	case searchResultsMsg:
		a.searchQuery = msg.query
		a.thList.searchResults = msg.results
		return a, nil
	case clearSearchMsg:
		a.searchQuery = ""
		a.thList.searchResults = nil
		a.thList.searching = false
		return a, nil
	case errMsg:
		return a, func() tea.Msg { return tui.ErrMsg{Err: msg.err} }
	case tui.PingResultMsg:
		if msg.Err == nil {
			a.connected = true
		}
	case refreshThreadsMsg:
		a.thList.loading = true
		return a, tea.Batch(a.fetchThreads(), a.thList.spinner.Tick)
	}

	// Delegate to active view for UI-only state.
	switch a.view {
	case viewThreads:
		var cmd tea.Cmd
		a.thList, cmd = a.thList.update(msg, a.threadList)
		return a, cmd
	case viewChat:
		var cmd tea.Cmd
		a.chat, cmd = a.chat.update(msg, a.chatMsgs)
		if a.chat.browsing {
			a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
		}
		return a, cmd
	case viewRead:
		var cmd tea.Cmd
		a.read, cmd = a.read.update(msg)
		return a, cmd
	}
	return a, nil
}

func (a *dmApp) startReadContent(msg dm.Message) tea.Cmd {
	sdk := a.chatSDK
	ctx := a.ctx
	width := 80
	if a.layout != nil {
		width = a.layout.Width
	}
	return tea.Batch(
		a.read.spinner.Tick,
		func() tea.Msg {
			contentHex := msg.Labels["content"]
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

func (a *dmApp) View() (string, string) {
	// Sub-views (chat/read) don't show the tab bar.
	switch a.view {
	case viewChat:
		return a.chat.viewContent(a.chatMsgs, a.previews, a.self, a.layout)
	case viewRead:
		return a.read.viewContent()
	}

	tabBar := renderTabBar(a.tab)

	switch a.tab {
	case tabSync:
		body, help := a.sync.viewContent(a.layout)
		return tabBar + body, help
	default:
		body, help := a.thList.viewContent(a.threadList, a.self, a.connected, a.unread, a.layout)
		return tabBar + body, help
	}
}

// Messages

type refreshThreadsMsg struct{}
type openThreadMsg struct{ thread dm.Thread }
type backToThreadsMsg struct{}
type openMessageMsg struct{ msg dm.Message }
type backToChatMsg struct{}
type errMsg struct{ err error }

type threadsLoadedMsg struct{ threads []dm.Thread }
type threadPreviewLoadedMsg struct {
	convID  string
	preview string
}
type newMessageMsg struct{ msg dm.Message }
type chatMessagesLoadedMsg struct{ messages []dm.Message }
type chatPreviewLoadedMsg struct {
	ref     reference.Reference
	preview string
}
type chatSendDoneMsg struct {
	ref  reference.Reference
	text string
	ts   int64
}
type contentLoadedMsg struct{ content string }
type executeSearchMsg struct{ query string }
type searchResultsMsg struct {
	query   string
	results []dm.SearchResult
}
type clearSearchMsg struct{}

func localConversationLabels(convID string, self, peer identity.PublicKey) map[string]string {
	return map[string]string{
		"conversation": convID,
		"dm_from":      hex.EncodeToString(self[:]),
		"dm_to":        hex.EncodeToString(peer[:]),
	}
}

const (
	localPreviewMaxBytes = 256
	localPreviewMaxLines = 4
)

func truncateLocalPreview(text string) string {
	data := []byte(text)
	truncated := false
	if len(data) > localPreviewMaxBytes {
		data = data[:localPreviewMaxBytes]
		truncated = true
	}
	lines := strings.SplitN(string(data), "\n", localPreviewMaxLines+1)
	if len(lines) > localPreviewMaxLines {
		lines = lines[:localPreviewMaxLines]
		truncated = true
	}
	result := strings.Join(lines, "\n")
	if truncated {
		result += "..."
	}
	return result
}
