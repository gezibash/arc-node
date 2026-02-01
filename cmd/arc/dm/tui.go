package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

type tuiView int

const (
	viewThreads tuiView = iota
	viewChat
	viewRead
)

type dmApp struct {
	ctx     context.Context
	client  *client.Client
	threads *dm.Threads
	self    identity.PublicKey
	layout  *tui.Layout

	// Centralized domain state.
	threadList []dm.Thread
	chatMsgs   []dm.Message
	previews   map[reference.Reference]string
	connected  bool

	// Current chat conversation SDK (nil when on threads view).
	chatSDK *dm.DM

	// View state.
	view   tuiView
	thList threadsView
	chat   chatView
	read   readView
}

func runTUI(ctx context.Context, c *client.Client, threads *dm.Threads, kp *identity.Keypair) error {
	self := kp.PublicKey()
	base := tui.NewBase(ctx, c, "dm", "")

	app := &dmApp{
		ctx:      ctx,
		client:   c,
		threads:  threads,
		self:     self,
		layout:   base.Layout,
		previews: make(map[reference.Reference]string),
		view:     viewThreads,
		thList:   newThreadsView(),
	}

	base = base.WithApp(app)
	return base.Run()
}

func (a *dmApp) CanQuit() bool {
	return a.view == viewThreads && !a.thList.prompting
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
	ctx := a.ctx
	ref := msg.Ref
	return func() tea.Msg {
		preview, err := sdk.Preview(ctx, msg)
		if err != nil {
			return chatPreviewLoadedMsg{ref: ref, preview: ""}
		}
		return chatPreviewLoadedMsg{ref: ref, preview: preview}
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
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		if a.view == viewChat {
			a.chat = onResizeChat(a.chat, a.layout)
			a.chat = rebuildChatViewport(a.chat, a.chatMsgs, a.previews, a.self, a.layout)
		}
		return a, nil

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
		var cmds []tea.Cmd
		if a.view == viewChat && a.chatSDK != nil {
			convID := msg.msg.Labels["conversation"]
			expectedConv := dm.ConversationID(a.self, a.chatSDK.PeerPublicKey())
			if convID == expectedConv {
				a.appendChatMessage(msg.msg)
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
				a.previews[msg.ref] = truncateLocalPreview(msg.text)
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
		a.view = viewChat
		a.chatSDK = sdk
		a.chatMsgs = nil
		a.layout.AppName = "dm · " + hex.EncodeToString(msg.thread.PeerPub[:4])
		a.chat = newChatView(a.layout)
		return a, tea.Batch(
			a.fetchChatMessages(),
			a.chat.spinner.Tick,
			a.chat.textareaBlink(),
		)
	case backToThreadsMsg:
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
		a.chat, cmd = a.chat.update(msg)
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
	switch a.view {
	case viewThreads:
		return a.thList.viewContent(a.threadList, a.self, a.connected, a.layout)
	case viewChat:
		return a.chat.viewContent(a.chatMsgs, a.previews, a.self, a.layout)
	case viewRead:
		return a.read.viewContent()
	}
	return "", ""
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
