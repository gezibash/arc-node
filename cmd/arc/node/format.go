package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/pkg/reference"
)

var hexPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// ContentLoader fetches raw content by reference.
type ContentLoader func(ctx context.Context, ref reference.Reference) ([]byte, error)

func formatEntry(w io.Writer, e *client.Entry, outputMode string, preview bool, ctx context.Context, loader ContentLoader) error {
	if outputMode == "json" {
		return formatEntryJSON(w, e, preview, ctx, loader)
	}
	return formatEntryText(w, e, preview, ctx, loader)
}

func truncateHexValue(v string) string {
	if hexPattern.MatchString(v) {
		return v[:8]
	}
	return v
}

func formatTextHeader(w io.Writer) {
	fmt.Fprintf(w, "%-10s %-14s %s\n", "REF", "AGE", "LABELS")
}

func formatEntryText(w io.Writer, e *client.Entry, preview bool, ctx context.Context, loader ContentLoader) error {
	refHex := reference.Hex(e.Ref)
	short := refHex[:8]
	ts := time.Unix(0, e.Timestamp)
	age := time.Since(ts).Truncate(time.Second)

	var parts []string
	for k, v := range e.Labels {
		parts = append(parts, k+"="+truncateHexValue(v))
	}
	slices.Sort(parts)

	fmt.Fprintf(w, "%-10s %-14s %s\n", short, formatDuration(age)+" ago", strings.Join(parts, " "))

	if preview && loader != nil {
		text, err := loadPreview(ctx, e.Labels, loader)
		if err == nil && text != "" {
			for _, line := range strings.Split(text, "\n") {
				fmt.Fprintf(w, "%-10s %-14s %s\n", "", "", line)
			}
		}
	}

	return nil
}

const (
	previewMaxBytes = 256
	previewMaxLines = 4
)

func loadPreview(ctx context.Context, labels map[string]string, loader ContentLoader) (string, error) {
	hex := labels["content"]
	if hex == "" {
		return "", fmt.Errorf("no _content label")
	}
	ref, err := reference.FromHex(hex)
	if err != nil {
		return "", err
	}
	contentData, err := loader(ctx, ref)
	if err != nil {
		return "", err
	}
	return truncatePreview(contentData), nil
}

func truncatePreview(data []byte) string {
	truncated := false
	if len(data) > previewMaxBytes {
		data = data[:previewMaxBytes]
		truncated = true
	}
	text := string(data)
	lines := strings.SplitN(text, "\n", previewMaxLines+1)
	if len(lines) > previewMaxLines {
		lines = lines[:previewMaxLines]
		truncated = true
	}
	result := strings.Join(lines, "\n")
	if truncated {
		result += "..."
	}
	return result
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
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

type jsonEntry struct {
	Reference string            `json:"reference"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Preview   string            `json:"preview,omitempty"`
}

func formatEntryJSON(w io.Writer, e *client.Entry, preview bool, ctx context.Context, loader ContentLoader) error {
	je := jsonEntry{
		Reference: reference.Hex(e.Ref),
		Labels:    e.Labels,
		Timestamp: e.Timestamp,
	}
	if preview && loader != nil {
		text, err := loadPreview(ctx, e.Labels, loader)
		if err == nil {
			je.Preview = text
		}
	}
	data, err := json.Marshal(je)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", data)
	return err
}
