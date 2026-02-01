package render

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Metadata describes the result set for agent-friendly CLI output.
type Metadata struct {
	Command      string       `json:"command" yaml:"command"`
	Query        string       `json:"query,omitempty" yaml:"query,omitempty"`
	TotalCount   int          `json:"total_count" yaml:"total_count"`
	ShowingCount int          `json:"showing_count" yaml:"showing_count"`
	Limit        int          `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset       int          `json:"offset,omitempty" yaml:"offset,omitempty"`
	HasMore      bool         `json:"has_more" yaml:"has_more"`
	NextCursor   string       `json:"next_cursor,omitempty" yaml:"next_cursor,omitempty"`
	TimeRange    *TimeRange   `json:"time_range,omitempty" yaml:"time_range,omitempty"`
	NextActions  []NextAction `json:"next_actions,omitempty" yaml:"next_actions,omitempty"`
	GeneratedAt  int64        `json:"generated_at" yaml:"generated_at"`
}

// TimeRange captures the oldest and newest timestamps in a result set.
type TimeRange struct {
	Oldest int64 `json:"oldest" yaml:"oldest"`
	Newest int64 `json:"newest" yaml:"newest"`
}

// NextAction suggests a follow-up command.
type NextAction struct {
	Description string `json:"description" yaml:"description"`
	Command     string `json:"command" yaml:"command"`
}

// MarkdownWithFrontmatter writes YAML frontmatter followed by content produced
// by contentFn. The YAML is hand-rolled (no dependency).
func MarkdownWithFrontmatter(w io.Writer, meta Metadata, contentFn func(io.Writer) error) error {
	meta.GeneratedAt = time.Now().UnixMilli()
	if meta.NextActions == nil {
		meta.NextActions = BuildNextActions(meta)
	}

	var werr error
	p := func(format string, args ...any) {
		if werr != nil {
			return
		}
		_, werr = fmt.Fprintf(w, format, args...)
	}
	pl := func(args ...any) {
		if werr != nil {
			return
		}
		_, werr = fmt.Fprintln(w, args...)
	}

	pl("---")
	p("command: %q\n", meta.Command)
	if meta.Query != "" {
		p("query: %q\n", meta.Query)
	}
	p("total_count: %d\n", meta.TotalCount)
	p("showing: %d\n", meta.ShowingCount)
	if meta.Limit > 0 {
		p("limit: %d\n", meta.Limit)
	}
	if meta.Offset > 0 {
		p("offset: %d\n", meta.Offset)
	}
	p("has_more: %t\n", meta.HasMore)
	if meta.NextCursor != "" {
		p("next_cursor: %q\n", meta.NextCursor)
	}
	if meta.TimeRange != nil {
		pl("time_range:")
		p("  oldest: %d\n", meta.TimeRange.Oldest)
		p("  newest: %d\n", meta.TimeRange.Newest)
	}
	if len(meta.NextActions) > 0 {
		pl("next_actions:")
		for _, a := range meta.NextActions {
			p("  - description: %q\n", a.Description)
			p("    command: %q\n", a.Command)
		}
	}
	p("generated_at: %d\n", meta.GeneratedAt)
	pl("---")
	pl()
	if werr != nil {
		return werr
	}
	return contentFn(w)
}

// JSONEnvelope writes a JSON object with metadata and results.
func JSONEnvelope(w io.Writer, meta Metadata, results any) error {
	meta.GeneratedAt = time.Now().UnixMilli()
	if meta.NextActions == nil {
		meta.NextActions = BuildNextActions(meta)
	}

	envelope := struct {
		Metadata Metadata `json:"metadata"`
		Results  any      `json:"results"`
	}{
		Metadata: meta,
		Results:  results,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(envelope)
}

// BuildNextActions generates navigation suggestions based on the metadata.
func BuildNextActions(meta Metadata) []NextAction {
	var actions []NextAction
	if meta.HasMore {
		if meta.NextCursor != "" {
			actions = append(actions, NextAction{
				Description: "Next page",
				Command:     fmt.Sprintf("%s --cursor %s", meta.Command, meta.NextCursor),
			})
		}
		if meta.Limit > 0 && meta.Offset >= 0 {
			actions = append(actions, NextAction{
				Description: "Next page (offset)",
				Command:     fmt.Sprintf("%s --offset %d", meta.Command, meta.Offset+meta.Limit),
			})
		}
	}
	return actions
}
