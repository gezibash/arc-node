package journal

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/gezibash/arc-node/cmd/arc/tui"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// entryItem wraps a journal entry as a list.Item.
type entryItem struct {
	entry   journal.Entry
	preview string
}

func (i entryItem) Title() string {
	var zeroRef reference.Reference
	short := reference.Hex(i.entry.Ref)[:8]
	if i.entry.EntryRef != zeroRef {
		short = reference.Hex(i.entry.EntryRef)[:8]
	}
	ts := time.UnixMilli(i.entry.Timestamp)
	return short + "  " + relativeTime(ts)
}

func (i entryItem) Description() string {
	if i.preview == "" {
		return ""
	}
	line := strings.SplitN(i.preview, "\n", 2)[0]
	if len(line) > 80 {
		line = line[:80] + "…"
	}
	return line
}

func (i entryItem) FilterValue() string {
	return i.preview
}

// entryDelegate renders journal entries in the list.
type entryDelegate struct{}

func (d entryDelegate) Height() int                             { return 2 }
func (d entryDelegate) Spacing() int                            { return 0 }
func (d entryDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }

func (d entryDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	e, ok := item.(entryItem)
	if !ok {
		return
	}

	var zeroRef reference.Reference
	short := reference.Hex(e.entry.Ref)[:8]
	if e.entry.EntryRef != zeroRef {
		short = reference.Hex(e.entry.EntryRef)[:8]
	}
	ts := time.UnixMilli(e.entry.Timestamp)
	age := relativeTime(ts)

	width := m.Width() - 4
	if width < 20 {
		width = 20
	}

	isSelected := index == m.Index()

	// Title line: ref + age
	var titleLine string
	if isSelected {
		titleLine = tui.CursorStyle.Render("▸") + " " +
			tui.RefStyle.Render(short) + "  " +
			tui.AgeStyle.Render(age)
	} else {
		titleLine = "  " +
			tui.RefStyle.Render(short) + "  " +
			tui.AgeStyle.Render(age)
	}

	// Description line: preview
	descLine := ""
	preview := e.preview
	if preview != "" {
		line := strings.SplitN(preview, "\n", 2)[0]
		maxLen := width
		if maxLen > 80 {
			maxLen = 80
		}
		if len(line) > maxLen {
			line = line[:maxLen] + "…"
		}
		if isSelected {
			descLine = "    " + lipgloss.NewStyle().Foreground(tui.DimColor).Bold(true).Render(line)
		} else {
			descLine = "    " + tui.PreviewStyle.Render(line)
		}
	}

	_, _ = fmt.Fprintf(w, "%s\n%s", titleLine, descLine)
}

// listView owns only UI-local state. Domain data (entries, previews) lives
// in journalApp.
type listView struct {
	model       list.Model
	loading     bool
	spinner     spinner.Model
	searching   bool
	searchInput textinput.Model
	ready       bool
}

func newListView() listView {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(tui.AccentColor)

	ti := textinput.New()
	ti.Placeholder = "search..."
	ti.CharLimit = 256
	ti.Width = 40

	delegate := entryDelegate{}
	l := list.New([]list.Item{}, delegate, 80, 20)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)
	l.SetShowHelp(false)
	l.SetFilteringEnabled(false)
	l.DisableQuitKeybindings()
	l.Styles.NoItems = lipgloss.NewStyle().Foreground(tui.DimColor)

	// Style pagination dots.
	l.Styles.ActivePaginationDot = lipgloss.NewStyle().Foreground(tui.AccentColor).SetString("•")
	l.Styles.InactivePaginationDot = lipgloss.NewStyle().Foreground(tui.DimColor).SetString("•")

	// Disable all built-in keybindings that conflict with ours.
	l.KeyMap.ShowFullHelp.SetEnabled(false)
	l.KeyMap.CloseFullHelp.SetEnabled(false)
	l.KeyMap.Filter.SetEnabled(false)
	l.KeyMap.ClearFilter.SetEnabled(false)
	l.KeyMap.AcceptWhileFiltering.SetEnabled(false)
	l.KeyMap.CancelWhileFiltering.SetEnabled(false)
	l.KeyMap.NextPage.SetEnabled(false)
	l.KeyMap.PrevPage.SetEnabled(false)

	return listView{
		model:       l,
		loading:     true,
		spinner:     s,
		searchInput: ti,
	}
}

func (v listView) update(msg tea.Msg, _ []journal.Entry) (listView, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		v.spinner, cmd = v.spinner.Update(msg)
		return v, cmd
	case tea.WindowSizeMsg:
		// Let the list model handle resize.
		v.model.SetSize(msg.Width-4, msg.Height-8)
		v.ready = true
	case tea.KeyMsg:
		if v.searching {
			switch msg.String() {
			case "esc":
				v.searching = false
				v.searchInput.Blur()
				v.searchInput.SetValue("")
				return v, func() tea.Msg { return clearSearchMsg{} }
			case "enter":
				query := strings.TrimSpace(v.searchInput.Value())
				if query == "" {
					v.searching = false
					v.searchInput.Blur()
					return v, func() tea.Msg { return clearSearchMsg{} }
				}
				v.searching = false
				v.searchInput.Blur()
				return v, func() tea.Msg { return executeSearchMsg{query: query} }
			default:
				var cmd tea.Cmd
				v.searchInput, cmd = v.searchInput.Update(msg)
				return v, cmd
			}
		}

		switch msg.String() {
		case "esc":
			return v, func() tea.Msg { return clearSearchMsg{} }
		case "enter":
			if item, ok := v.model.SelectedItem().(entryItem); ok {
				return v, func() tea.Msg {
					return openEntryMsg{entry: item.entry}
				}
			}
		case "n":
			return v, func() tea.Msg { return openWriteMsg{} }
		case "r":
			v.loading = true
			return v, func() tea.Msg { return refreshEntriesMsg{} }
		case "/":
			v.searching = true
			v.searchInput.SetValue("")
			v.searchInput.Focus()
			return v, v.searchInput.Cursor.BlinkCmd()
		default:
			// Auto-load more when navigating near the bottom.
			if (msg.String() == "j" || msg.String() == "down") && !v.loading {
				if v.model.Index() >= len(v.model.Items())-2 {
					v.loading = true
					var cmd tea.Cmd
					v.model, cmd = v.model.Update(msg)
					return v, tea.Batch(cmd, func() tea.Msg { return loadMoreMsg{} })
				}
			}
			var cmd tea.Cmd
			v.model, cmd = v.model.Update(msg)
			return v, cmd
		}
	}

	var cmd tea.Cmd
	v.model, cmd = v.model.Update(msg)
	return v, cmd
}

func (v *listView) setItems(items []list.Item) {
	v.model.SetItems(items)
}

func (v listView) viewContent(_ []journal.Entry, _ map[reference.Reference]string, _ map[reference.Reference]string, searchQuery string, _ bool, layout *tui.Layout) (string, string) {
	helpText := "↑/k up • ↓/j down • enter: read • n: new • /: search • r: refresh • tab: sync • esc: quit"

	if !v.ready && layout != nil {
		w, h := layout.BodySize()
		v.model.SetSize(w-4, h)
		v.ready = true
	}

	var b strings.Builder

	if v.searching {
		helpText = "enter: search • esc: cancel"
		b.WriteString("  " + tui.SubtitleStyle.Render("/") + " " + v.searchInput.View() + "\n\n")
	} else if searchQuery != "" {
		helpText = "↑/k up • ↓/j down • enter: read • /: new search • esc: back"
		b.WriteString("  " + tui.SubtitleStyle.Render("search: "+searchQuery) + "\n\n")
	}

	if v.loading {
		b.WriteString(fmt.Sprintf("%s Loading entries...\n", v.spinner.View()))
		return b.String(), helpText
	}

	b.WriteString(v.model.View())

	return b.String(), helpText
}
