package render

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

var hexPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// TruncateHexValue shortens 64-char hex strings to 8 chars for display.
func TruncateHexValue(v string) string {
	if hexPattern.MatchString(v) {
		return v[:8]
	}
	return v
}

// FormatDuration returns a compact human-readable duration.
func FormatDuration(d time.Duration) string {
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

// RelativeTime returns a human-friendly relative time string.
func RelativeTime(ts time.Time) string {
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

// WriteJSON writes v as indented JSON to w.
func WriteJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ParseLabels splits "key=value" strings into a map.
func ParseLabels(labels []string) (map[string]string, error) {
	m := make(map[string]string, len(labels))
	for _, l := range labels {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid label format %q, expected key=value", l)
		}
		m[parts[0]] = parts[1]
	}
	return m, nil
}

// ReadInput reads data from the first positional arg — treated as a file path
// if it exists on disk, otherwise as literal text. "-" reads stdin explicitly.
// With no args, reads stdin.
func ReadInput(args []string) ([]byte, error) {
	if len(args) == 1 {
		if args[0] == "-" {
			return io.ReadAll(os.Stdin)
		}
		data, err := os.ReadFile(args[0])
		if err == nil {
			return data, nil
		}
		// Not a valid file path — treat as literal text.
		return []byte(args[0]), nil
	}
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, fmt.Errorf("read input: %w", err)
	}
	return data, nil
}
