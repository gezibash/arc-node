package dm

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/gezibash/arc-node/cmd/arc/render"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/reference"
)

// --- Text ---

func formatListText(w io.Writer, result *dm.ListResult, preview bool, sdk *dm.DM, self identity.PublicKey) error {
	if len(result.Messages) > 0 {
		_, _ = fmt.Fprintf(w, "%-10s %-8s %-14s %s\n", "REF", "FROM", "AGE", "LABELS")
	}
	for _, m := range result.Messages {
		formatMessageText(w, m, preview, sdk, self)
	}
	if result.HasMore {
		_, _ = fmt.Fprintf(w, "next cursor: %s\n", result.NextCursor)
	}
	return nil
}

func formatMessageText(w io.Writer, m dm.Message, preview bool, sdk *dm.DM, self identity.PublicKey) {
	short := reference.Hex(m.Ref)[:8]
	ts := time.UnixMilli(m.Timestamp)
	age := time.Since(ts).Truncate(time.Second)
	sender := senderLabel(m.From, self)

	var parts []string
	for k, v := range m.Labels {
		if k == "app" || k == "type" || k == "conversation" || k == "from" || k == "to" || k == "dm_from" || k == "dm_to" || k == "content" {
			continue
		}
		parts = append(parts, k+"="+truncateHexValue(v))
	}
	slices.Sort(parts)

	_, _ = fmt.Fprintf(w, "%-10s %-8s %-14s %s\n", short, sender, formatDuration(age)+" ago", strings.Join(parts, " "))

	if preview && sdk != nil {
		text, err := sdk.Preview(context.Background(), m)
		if err == nil && text != "" {
			for _, line := range strings.Split(text, "\n") {
				_, _ = fmt.Fprintf(w, "%-10s %-8s %-14s %s\n", "", "", "", line)
			}
		}
	}
}

// --- JSON ---

type jsonMessage struct {
	Reference string            `json:"reference"`
	From      string            `json:"from"`
	To        string            `json:"to"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Preview   string            `json:"preview,omitempty"`
}

func formatListJSON(w io.Writer, result *dm.ListResult, preview bool, sdk *dm.DM, self identity.PublicKey) error {
	var jsonResults []jsonMessage
	for _, m := range result.Messages {
		jm := jsonMessage{
			Reference: reference.Hex(m.Ref),
			From:      senderLabel(m.From, self),
			To:        senderLabel(m.To, self),
			Labels:    m.Labels,
			Timestamp: m.Timestamp,
		}
		if preview && sdk != nil {
			text, err := sdk.Preview(context.Background(), m)
			if err == nil {
				jm.Preview = text
			}
		}
		jsonResults = append(jsonResults, jm)
	}

	meta := buildDMMeta("arc dm list", result)
	return render.JSONEnvelope(w, meta, jsonResults)
}

// --- Markdown ---

func formatListMarkdown(w io.Writer, result *dm.ListResult, preview bool, sdk *dm.DM, self identity.PublicKey) error {
	meta := buildDMMeta("arc dm list", result)
	return render.MarkdownWithFrontmatter(w, meta, func(w io.Writer) error {
		_, _ = fmt.Fprintln(w, "## Conversation")
		_, _ = fmt.Fprintln(w)

		for _, m := range result.Messages {
			ts := time.UnixMilli(m.Timestamp)
			short := reference.Hex(m.Ref)[:8]
			sender := senderLabel(m.From, self)
			_, _ = fmt.Fprintf(w, "### %s — %s [%s]\n", sender, ts.Format("2006-01-02 15:04"), short)

			if preview && sdk != nil {
				text, err := sdk.Preview(context.Background(), m)
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
			_, _ = fmt.Fprintf(w, "_More messages available (cursor: %s)_\n", result.NextCursor)
		}
		return nil
	})
}

// runThreadsMarkdown is the default non-TTY handler for bare `arc dm`.
func runThreadsMarkdown(ctx context.Context, threads *dm.Threads, kp *identity.Keypair, w io.Writer) error {
	self := kp.PublicKey()
	items, err := threads.ListThreads(ctx)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintln(w, "## Conversations")
	_, _ = fmt.Fprintln(w)

	if len(items) == 0 {
		_, _ = fmt.Fprintln(w, "No conversations yet.")
		return nil
	}

	for _, th := range items {
		peerHex := hex.EncodeToString(th.PeerPub[:])
		peerShort := peerHex[:8]
		ts := time.UnixMilli(th.LastMsg.Timestamp)
		sender := senderLabel(th.LastMsg.From, self)
		_, _ = fmt.Fprintf(w, "- **%s** — last: %s (%s) `%s`\n", peerShort, sender, ts.Format("2006-01-02 15:04"), peerHex)
	}
	_, _ = fmt.Fprintln(w)
	return nil
}

// --- Metadata ---

func buildDMMeta(command string, result *dm.ListResult) render.Metadata {
	meta := render.Metadata{
		Command:      command,
		TotalCount:   len(result.Messages),
		ShowingCount: len(result.Messages),
		HasMore:      result.HasMore,
		NextCursor:   result.NextCursor,
	}
	if len(result.Messages) > 0 {
		oldest := result.Messages[0].Timestamp
		newest := result.Messages[0].Timestamp
		for _, m := range result.Messages[1:] {
			if m.Timestamp < oldest {
				oldest = m.Timestamp
			}
			if m.Timestamp > newest {
				newest = m.Timestamp
			}
		}
		meta.TimeRange = &render.TimeRange{Oldest: oldest, Newest: newest}
	}
	return meta
}

// --- Helpers ---

func truncateHexValue(v string) string {
	return render.TruncateHexValue(v)
}

func formatDuration(d time.Duration) string {
	return render.FormatDuration(d)
}
