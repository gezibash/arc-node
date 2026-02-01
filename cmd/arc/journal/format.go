package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/pkg/reference"
)

// --- Text ---

func formatListText(w io.Writer, result *journal.ListResult, preview bool, sdk *journal.Journal) error {
	if len(result.Entries) > 0 {
		fmt.Fprintf(w, "%-10s %-14s %s\n", "REF", "AGE", "LABELS")
	}
	for _, e := range result.Entries {
		formatEntryText(w, e, preview, sdk)
	}
	if result.HasMore {
		fmt.Fprintf(w, "next cursor: %s\n", result.NextCursor)
	}
	return nil
}

func formatEntryText(w io.Writer, e journal.Entry, preview bool, sdk *journal.Journal) {
	short := reference.Hex(e.Ref)[:8]
	ts := time.UnixMilli(e.Timestamp)
	age := time.Since(ts).Truncate(time.Second)

	var parts []string
	for k, v := range e.Labels {
		parts = append(parts, k+"="+truncateHexValue(v))
	}
	slices.Sort(parts)

	fmt.Fprintf(w, "%-10s %-14s %s\n", short, formatDuration(age)+" ago", strings.Join(parts, " "))

	if preview && sdk != nil {
		text, err := sdk.Preview(context.Background(), e)
		if err == nil && text != "" {
			for _, line := range strings.Split(text, "\n") {
				fmt.Fprintf(w, "%-10s %-14s %s\n", "", "", line)
			}
		}
	}
}

// --- JSON ---

type jsonEntry struct {
	Reference string            `json:"reference"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Preview   string            `json:"preview,omitempty"`
}

func formatListJSON(w io.Writer, result *journal.ListResult, preview bool, sdk *journal.Journal) error {
	for _, e := range result.Entries {
		je := jsonEntry{
			Reference: reference.Hex(e.Ref),
			Labels:    e.Labels,
			Timestamp: e.Timestamp,
		}
		if preview && sdk != nil {
			text, err := sdk.Preview(context.Background(), e)
			if err == nil {
				je.Preview = text
			}
		}
		data, err := json.Marshal(je)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s\n", data)
	}
	return nil
}

// --- Markdown ---

func formatListMarkdown(w io.Writer, result *journal.ListResult, preview bool, sdk *journal.Journal) error {
	fmt.Fprintln(w, "## Journal Entries")
	fmt.Fprintln(w)

	for _, e := range result.Entries {
		ts := time.UnixMilli(e.Timestamp)
		short := reference.Hex(e.Ref)[:8]
		fmt.Fprintf(w, "### %s [%s]\n", ts.Format("2006-01-02 15:04"), short)

		var labelParts []string
		for k, v := range e.Labels {
			if k == "app" || k == "type" || k == "content" {
				continue
			}
			labelParts = append(labelParts, k+"="+truncateHexValue(v))
		}
		slices.Sort(labelParts)
		if len(labelParts) > 0 {
			fmt.Fprintf(w, "Labels: %s\n", strings.Join(labelParts, ", "))
		}

		if preview && sdk != nil {
			text, err := sdk.Preview(context.Background(), e)
			if err == nil && text != "" {
				fmt.Fprintln(w)
				fmt.Fprintln(w, text)
			}
		}

		fmt.Fprintln(w)
		fmt.Fprintln(w, "---")
		fmt.Fprintln(w)
	}

	if result.HasMore {
		fmt.Fprintf(w, "_More entries available (cursor: %s)_\n", result.NextCursor)
	}
	return nil
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
	return formatListMarkdown(w, result, false, nil)
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
