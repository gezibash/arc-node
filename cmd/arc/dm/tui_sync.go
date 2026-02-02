package dm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/reference"
)

type syncStatus int

const (
	syncIdle syncStatus = iota
	syncFetching
	syncPulling
	syncPushing
	syncReindexing
)

type confirmAction int

const (
	confirmNone confirmAction = iota
	confirmPull
	confirmPush
)

type syncView struct {
	localHash        string
	localCount       int
	localLastUpdate  int64
	remoteHash       string
	remoteCount      int
	remoteLastUpdate int64
	threadCount      int
	status           syncStatus
	confirm          confirmAction
	message          string
	spinner          spinner.Model
	state            *dm.SearchState
}

func newSyncView(searchDir string) syncView {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)
	return syncView{
		spinner: s,
		state:   dm.LoadSearchState(searchDir),
	}
}

func (v syncView) update(msg tea.Msg) (syncView, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		v.spinner, cmd = v.spinner.Update(msg)
		return v, cmd
	case syncFetchedMsg:
		v.status = syncIdle
		if msg.err != nil {
			v.message = fmt.Sprintf("fetch failed: %v", msg.err)
		} else if msg.info == nil {
			v.message = "no remote index"
			v.remoteHash = ""
			v.remoteCount = 0
			v.remoteLastUpdate = 0
		} else {
			v.remoteHash = msg.info.DBHash
			v.remoteCount = msg.info.EntryCount
			v.remoteLastUpdate = msg.info.LastUpdate
			v.state.RemoteHash = msg.info.DBHash
			_ = v.state.Save()
			if v.localHash == v.remoteHash {
				v.message = "up to date"
			} else {
				v.message = "remote differs"
			}
		}
		return v, nil
	case syncPulledMsg:
		v.status = syncIdle
		if msg.err != nil {
			v.message = fmt.Sprintf("pull failed: %v", msg.err)
		} else {
			v.localHash = msg.localHash
			v.localCount = msg.count
			v.localLastUpdate = msg.lastUpdate
			v.remoteHash = msg.remoteHash
			v.state.LocalHash = msg.localHash
			v.state.RemoteHash = msg.remoteHash
			_ = v.state.Save()
			v.message = fmt.Sprintf("pulled (%d entries)", msg.count)
		}
		return v, nil
	case syncPushedMsg:
		v.status = syncIdle
		if msg.err != nil {
			v.message = fmt.Sprintf("push failed: %v", msg.err)
		} else {
			v.localHash = msg.hash
			v.remoteHash = msg.hash
			v.state.LocalHash = msg.hash
			v.state.RemoteHash = msg.hash
			_ = v.state.Save()
			v.message = fmt.Sprintf("pushed %s", msg.ref)
		}
		return v, nil
	case syncReindexedMsg:
		v.status = syncIdle
		if msg.err != nil {
			v.message = fmt.Sprintf("reindex failed: %v", msg.err)
		} else {
			v.localHash = msg.hash
			v.localCount = msg.count
			v.localLastUpdate = msg.lastUpdate
			v.message = fmt.Sprintf("reindexed (%d messages)", msg.count)
		}
		return v, nil
	}
	return v, nil
}

func shortTime(ms int64) string {
	if ms == 0 {
		return "—"
	}
	return time.UnixMilli(ms).Format("Jan 2 15:04")
}

func (v syncView) viewContent(_ *tui.Layout) (string, string) {
	helpText := "f: fetch • p: pull • P: push • r: reindex • tab: threads • esc: quit"

	var b strings.Builder

	b.WriteString("  " + tui.GroupHeaderStyle.Render("Search Index") + "\n\n")

	if v.threadCount > 0 {
		b.WriteString(fmt.Sprintf("    %d threads\n\n", v.threadCount))
	}

	localShort := "(empty)"
	if v.localHash != "" {
		localShort = v.localHash[:8]
	}
	remoteShort := "(unknown)"
	if v.remoteHash != "" {
		remoteShort = v.remoteHash[:8]
	}

	b.WriteString(fmt.Sprintf("    local   %s  %d entries  %s\n",
		tui.RefStyle.Render(localShort), v.localCount, tui.SubtitleStyle.Render(shortTime(v.localLastUpdate))))
	b.WriteString(fmt.Sprintf("    remote  %s  %d entries  %s\n",
		tui.RefStyle.Render(remoteShort), v.remoteCount, tui.SubtitleStyle.Render(shortTime(v.remoteLastUpdate))))
	b.WriteString("\n")

	if v.localHash != "" && v.remoteHash != "" {
		if v.localHash == v.remoteHash {
			b.WriteString("    " + lipgloss.NewStyle().Foreground(tui.GreenColor).Render("✓ in sync") + "\n")
		} else {
			b.WriteString("    " + lipgloss.NewStyle().Foreground(tui.WarnColor).Render("✗ out of sync") + "\n")
		}
	} else {
		b.WriteString("    " + tui.SubtitleStyle.Render("press f to fetch remote status") + "\n")
	}
	b.WriteString("\n")

	if v.status != syncIdle {
		var label string
		switch v.status {
		case syncFetching:
			label = "fetching..."
		case syncPulling:
			label = "pulling..."
		case syncPushing:
			label = "pushing..."
		case syncReindexing:
			label = "reindexing..."
		}
		b.WriteString("    " + v.spinner.View() + " " + label + "\n")
	} else if v.message != "" {
		b.WriteString("    " + tui.SubtitleStyle.Render(v.message) + "\n")
	}

	return b.String(), helpText
}

// Messages for sync operations.
type syncFetchedMsg struct {
	info *dm.RemoteSearchInfo
	err  error
}
type syncPulledMsg struct {
	localHash  string
	remoteHash string
	count      int
	lastUpdate int64
	err        error
}
type syncPushedMsg struct {
	ref  string
	hash string
	err  error
}
type syncReindexedMsg struct {
	hash       string
	count      int
	lastUpdate int64
	err        error
}

// Commands — these are created from dmApp which has access to the SDK.

func fetchSyncCmd(ctx context.Context, threads *dm.Threads) tea.Cmd {
	return func() tea.Msg {
		info, err := threads.FetchRemoteSearchInfo(ctx)
		return syncFetchedMsg{info: info, err: err}
	}
}

func pullSyncCmd(ctx context.Context, threads *dm.Threads, searchPath string) tea.Cmd {
	return func() tea.Msg {
		idx := threads.SearchIndex()
		if idx != nil {
			_ = idx.Close()
		}
		pulled, err := threads.PullSearchIndex(ctx, searchPath)
		if err != nil {
			reopened, _ := dm.OpenSearchIndex(ctx, searchPath)
			threads.SetSearchIndex(reopened)
			return syncPulledMsg{err: err}
		}
		threads.SetSearchIndex(pulled)

		hash, _ := pulled.ContentHash(context.Background())
		var count int
		_ = pulled.Count(context.Background(), &count)
		ts, _ := pulled.LastIndexedTimestamp(context.Background())

		info, _ := threads.FetchRemoteSearchInfo(ctx)
		remoteHash := ""
		if info != nil {
			remoteHash = info.DBHash
		}

		return syncPulledMsg{
			localHash:  reference.Hex(hash),
			remoteHash: remoteHash,
			count:      count,
			lastUpdate: ts,
		}
	}
}

func pushSyncCmd(ctx context.Context, threads *dm.Threads) tea.Cmd {
	return func() tea.Msg {
		idx := threads.SearchIndex()
		if idx == nil {
			return syncPushedMsg{err: fmt.Errorf("no search index")}
		}
		ref, err := threads.PushSearchIndex(ctx, idx)
		if err != nil {
			return syncPushedMsg{err: err}
		}
		hash, _ := idx.ContentHash(context.Background())
		return syncPushedMsg{
			ref:  reference.Hex(ref)[:8],
			hash: reference.Hex(hash),
		}
	}
}

func reindexSyncCmd(ctx context.Context, threads *dm.Threads) tea.Cmd {
	return func() tea.Msg {
		result, err := threads.Reindex(ctx)
		if err != nil {
			return syncReindexedMsg{err: err}
		}
		if result.Errors > 0 {
			return syncReindexedMsg{err: fmt.Errorf("indexed %d, %d errors", result.Indexed, result.Errors)}
		}
		idx := threads.SearchIndex()
		hash, _ := idx.ContentHash(context.Background())
		var count int
		_ = idx.Count(context.Background(), &count)
		ts, _ := idx.LastIndexedTimestamp(context.Background())
		return syncReindexedMsg{
			hash:       reference.Hex(hash),
			count:      count,
			lastUpdate: ts,
		}
	}
}
