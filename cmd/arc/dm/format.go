package dm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/reference"
)

// --- Text ---

func formatListText(w io.Writer, result *dm.ListResult, preview bool, sdk *dm.DM, self identity.PublicKey) error {
	if len(result.Messages) > 0 {
		fmt.Fprintf(w, "%-10s %-8s %-14s %s\n", "REF", "FROM", "AGE", "LABELS")
	}
	for _, m := range result.Messages {
		formatMessageText(w, m, preview, sdk, self)
	}
	if result.HasMore {
		fmt.Fprintf(w, "next cursor: %s\n", result.NextCursor)
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

	fmt.Fprintf(w, "%-10s %-8s %-14s %s\n", short, sender, formatDuration(age)+" ago", strings.Join(parts, " "))

	if preview && sdk != nil {
		text, err := sdk.Preview(context.Background(), m)
		if err == nil && text != "" {
			for _, line := range strings.Split(text, "\n") {
				fmt.Fprintf(w, "%-10s %-8s %-14s %s\n", "", "", "", line)
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
		data, err := json.Marshal(jm)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s\n", data)
	}
	return nil
}

// --- Markdown ---

func formatListMarkdown(w io.Writer, result *dm.ListResult, preview bool, sdk *dm.DM, self identity.PublicKey) error {
	fmt.Fprintln(w, "## Conversation")
	fmt.Fprintln(w)

	for _, m := range result.Messages {
		ts := time.UnixMilli(m.Timestamp)
		short := reference.Hex(m.Ref)[:8]
		sender := senderLabel(m.From, self)
		fmt.Fprintf(w, "### %s — %s [%s]\n", sender, ts.Format("2006-01-02 15:04"), short)

		if preview && sdk != nil {
			text, err := sdk.Preview(context.Background(), m)
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
		fmt.Fprintf(w, "_More messages available (cursor: %s)_\n", result.NextCursor)
	}
	return nil
}

// runThreadsMarkdown is the default non-TTY handler for bare `arc dm`.
func runThreadsMarkdown(ctx context.Context, threads *dm.Threads, kp *identity.Keypair, w io.Writer) error {
	self := kp.PublicKey()
	items, err := threads.ListThreads(ctx)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "## Conversations")
	fmt.Fprintln(w)

	if len(items) == 0 {
		fmt.Fprintln(w, "No conversations yet.")
		return nil
	}

	for _, th := range items {
		peerHex := hex.EncodeToString(th.PeerPub[:])
		peerShort := peerHex[:8]
		ts := time.UnixMilli(th.LastMsg.Timestamp)
		sender := senderLabel(th.LastMsg.From, self)
		fmt.Fprintf(w, "- **%s** — last: %s (%s) `%s`\n", peerShort, sender, ts.Format("2006-01-02 15:04"), peerHex)
	}
	fmt.Fprintln(w)
	return nil
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
