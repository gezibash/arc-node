package journal

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gezibash/arc-node/cmd/arc/render"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// --- Text ---

func formatListText(w io.Writer, result *journal.ListResult, preview, content bool, sdk *journal.Journal) error {
	if len(result.Entries) > 0 {
		_, _ = fmt.Fprintf(w, "%-10s %-14s %s\n", "REF", "AGE", "LABELS")
	}
	for _, e := range result.Entries {
		formatEntryText(w, e, preview || content, sdk)
		if content && sdk != nil {
			text, err := readFullContent(sdk, e)
			if err == nil && text != "" {
				truncated := text
				if len(truncated) > 500 {
					truncated = truncated[:500] + "..."
				}
				for _, line := range strings.Split(truncated, "\n") {
					_, _ = fmt.Fprintf(w, "%-10s %-14s %s\n", "", "", line)
				}
			}
		}
	}
	if result.HasMore {
		_, _ = fmt.Fprintf(w, "next cursor: %s\n", result.NextCursor)
	}
	return nil
}

func entryShortRef(e journal.Entry) string {
	var zeroRef reference.Reference
	if e.EntryRef != zeroRef {
		return reference.Hex(e.EntryRef)[:8]
	}
	return reference.Hex(e.Ref)[:8]
}

func formatEntryText(w io.Writer, e journal.Entry, preview bool, sdk *journal.Journal) {
	short := entryShortRef(e)
	ts := time.UnixMilli(e.Timestamp)
	age := time.Since(ts).Truncate(time.Second)

	var parts []string
	for k, v := range e.Labels {
		parts = append(parts, k+"="+truncateHexValue(v))
	}
	slices.Sort(parts)

	_, _ = fmt.Fprintf(w, "%-10s %-14s %s\n", short, formatDuration(age)+" ago", strings.Join(parts, " "))

	if preview && sdk != nil {
		text, err := sdk.Preview(context.Background(), e)
		if err == nil && text != "" {
			for _, line := range strings.Split(text, "\n") {
				_, _ = fmt.Fprintf(w, "%-10s %-14s %s\n", "", "", line)
			}
		}
	}
}

// --- JSON ---

type jsonEntry struct {
	Reference string            `json:"reference"`
	EntryRef  string            `json:"entry_ref,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Age       string            `json:"age"`
	Preview   string            `json:"preview,omitempty"`
	Content   string            `json:"content,omitempty"`
}

func formatListJSON(w io.Writer, result *journal.ListResult, preview, content bool, sdk *journal.Journal) error {
	var jsonResults []jsonEntry
	for _, e := range result.Entries {
		ts := time.UnixMilli(e.Timestamp)
		var zeroRef reference.Reference
		je := jsonEntry{
			Reference: reference.Hex(e.Ref),
			Labels:    e.Labels,
			Timestamp: e.Timestamp,
			Age:       relativeTime(ts),
		}
		if e.EntryRef != zeroRef {
			je.EntryRef = reference.Hex(e.EntryRef)
		}
		if content && sdk != nil {
			text, err := readFullContent(sdk, e)
			if err == nil {
				je.Content = text
			}
		} else if preview && sdk != nil {
			text, err := sdk.Preview(context.Background(), e)
			if err == nil {
				je.Preview = text
			}
		}
		jsonResults = append(jsonResults, je)
	}

	meta := buildListMeta("arc journal list", result)
	return render.JSONEnvelope(w, meta, jsonResults)
}

// --- Markdown ---

func formatListMarkdown(w io.Writer, result *journal.ListResult, preview, content bool, sdk *journal.Journal) error {
	meta := buildListMeta("arc journal list", result)
	return render.MarkdownWithFrontmatter(w, meta, func(w io.Writer) error {
		_, _ = fmt.Fprintln(w, "## Journal Entries")
		_, _ = fmt.Fprintln(w)

		for _, e := range result.Entries {
			ts := time.UnixMilli(e.Timestamp)
			short := entryShortRef(e)
			_, _ = fmt.Fprintf(w, "### %s [%s]\n", ts.Format("2006-01-02 15:04"), short)

			var labelParts []string
			for k, v := range e.Labels {
				if k == "app" || k == "type" || k == "content" {
					continue
				}
				labelParts = append(labelParts, k+"="+truncateHexValue(v))
			}
			slices.Sort(labelParts)
			if len(labelParts) > 0 {
				_, _ = fmt.Fprintf(w, "Labels: %s\n", strings.Join(labelParts, ", "))
			}

			if content && sdk != nil {
				text, err := readFullContent(sdk, e)
				if err == nil && text != "" {
					_, _ = fmt.Fprintln(w)
					_, _ = fmt.Fprintln(w, text)
				}
			} else if preview && sdk != nil {
				text, err := sdk.Preview(context.Background(), e)
				if err == nil && text != "" {
					_, _ = fmt.Fprintln(w)
					_, _ = fmt.Fprintln(w, text)
				}
			}

			_, _ = fmt.Fprintln(w)
			_, _ = fmt.Fprintln(w, "---")
			_, _ = fmt.Fprintln(w)
		}

		if result.HasMore {
			_, _ = fmt.Fprintf(w, "_More entries available (cursor: %s)_\n", result.NextCursor)
		}
		return nil
	})
}

// runListMarkdown is the default non-TTY handler for bare `arc journal`.
func runListMarkdown(ctx context.Context, sdk *journal.Journal, w io.Writer) error {
	result, err := sdk.List(ctx, journal.ListOptions{
		Limit:      10,
		Descending: true,
	})
	if err != nil {
		return err
	}
	return formatListMarkdown(w, result, true, false, sdk)
}

// readFullContent fetches and decrypts the full content of an entry.
func readFullContent(sdk *journal.Journal, e journal.Entry) (string, error) {
	hexStr := e.Labels["content"]
	if hexStr == "" {
		return "", fmt.Errorf("no content label")
	}
	ref, err := reference.FromHex(hexStr)
	if err != nil {
		return "", err
	}
	entry, err := sdk.Read(context.Background(), ref)
	if err != nil {
		return "", err
	}
	return string(entry.Content), nil
}

// --- Metadata ---

func buildListMeta(command string, result *journal.ListResult) render.Metadata {
	meta := render.Metadata{
		Command:      command,
		TotalCount:   len(result.Entries),
		ShowingCount: len(result.Entries),
		HasMore:      result.HasMore,
		NextCursor:   result.NextCursor,
	}
	if len(result.Entries) > 0 {
		oldest := result.Entries[0].Timestamp
		newest := result.Entries[0].Timestamp
		for _, e := range result.Entries[1:] {
			if e.Timestamp < oldest {
				oldest = e.Timestamp
			}
			if e.Timestamp > newest {
				newest = e.Timestamp
			}
		}
		meta.TimeRange = &render.TimeRange{Oldest: oldest, Newest: newest}
	}
	return meta
}

// --- Helpers ---

var hexPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

func truncateHexValue(v string) string {
	if hexPattern.MatchString(v) {
		return v[:8]
	}
	return v
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	h := int(d.Hours())
	m = m % 60
	return fmt.Sprintf("%dh %dm %ds", h, m, s)
}

// relativeTime returns a human-friendly relative time string that doesn't
// show seconds (to avoid constant visual updates in the TUI).
func relativeTime(ts time.Time) string {
	d := time.Since(ts)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		m := int(d.Minutes())
		if m == 1 {
			return "1m ago"
		}
		return fmt.Sprintf("%dm ago", m)
	case d < 24*time.Hour:
		h := int(d.Hours())
		if h == 1 {
			return "1h ago"
		}
		return fmt.Sprintf("%dh ago", h)
	case d < 48*time.Hour:
		return "yesterday"
	case d < 7*24*time.Hour:
		days := int(d.Hours() / 24)
		return fmt.Sprintf("%dd ago", days)
	case d < 30*24*time.Hour:
		weeks := int(d.Hours() / 24 / 7)
		if weeks == 1 {
			return "1 week ago"
		}
		return fmt.Sprintf("%d weeks ago", weeks)
	case d < 365*24*time.Hour:
		months := int(d.Hours() / 24 / 30)
		if months == 1 {
			return "1 month ago"
		}
		return fmt.Sprintf("%d months ago", months)
	default:
		return ts.Format("Jan 2, 2006")
	}
}
