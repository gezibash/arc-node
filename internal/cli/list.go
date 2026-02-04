package cli

import (
	"io"
	"strings"

	"github.com/jedib0t/go-pretty/v6/list"
)

// List renders a list of items using go-pretty.
// Created via Output.List().
type List struct {
	out   *Output
	meta  Meta
	items []Renderable
}

// Add appends items to the list.
func (l *List) Add(items ...Renderable) *List {
	l.items = append(l.items, items...)
	return l
}

// WithPagination sets pagination cursor and hasMore flag.
func (l *List) WithPagination(cursor string, hasMore bool) *List {
	l.meta = l.meta.WithPagination(cursor, hasMore)
	return l
}

// Render outputs the list in the configured format.
func (l *List) Render() error {
	return l.out.Render(l)
}

// Meta returns the list metadata.
func (l *List) Meta() Meta {
	return l.meta
}

// RenderText writes an ASCII list using go-pretty.
func (l *List) RenderText(w io.Writer) error {
	lw := list.NewWriter()
	lw.SetStyle(list.StyleBulletCircle)

	for _, item := range l.items {
		lw.AppendItem(renderableToString(item, FormatText))
	}

	_, err := io.WriteString(w, lw.Render()+"\n")
	return err
}

// RenderJSON returns the data as an array.
func (l *List) RenderJSON() any {
	result := make([]any, 0, len(l.items))
	for _, item := range l.items {
		result = append(result, item.RenderJSON())
	}
	return result
}

// RenderMarkdown writes a markdown list using go-pretty.
func (l *List) RenderMarkdown(w io.Writer) error {
	lw := list.NewWriter()
	lw.SetStyle(list.StyleMarkdown)

	for _, item := range l.items {
		lw.AppendItem(renderableToString(item, FormatMarkdown))
	}

	_, err := io.WriteString(w, lw.RenderMarkdown()+"\n")
	return err
}

// renderableToString renders a Renderable to string for embedding in lists.
func renderableToString(r Renderable, format Format) string {
	switch format {
	case FormatMarkdown:
		return renderToString(r.RenderMarkdown)
	default:
		return renderToString(r.RenderText)
	}
}

// renderToString captures output to a string.
func renderToString(fn func(io.Writer) error) string {
	var buf strings.Builder
	_ = fn(&buf)
	return strings.TrimSpace(buf.String())
}

// StringList is a simple list of strings.
// Created via Output.StringList().
type StringList struct {
	out   *Output
	meta  Meta
	items []string
}

// Add appends strings to the list.
func (l *StringList) Add(items ...string) *StringList {
	l.items = append(l.items, items...)
	return l
}

// WithPagination sets pagination cursor and hasMore flag.
func (l *StringList) WithPagination(cursor string, hasMore bool) *StringList {
	l.meta = l.meta.WithPagination(cursor, hasMore)
	return l
}

// Render outputs the list in the configured format.
func (l *StringList) Render() error {
	return l.out.Render(l)
}

// Meta returns the list metadata.
func (l *StringList) Meta() Meta {
	return l.meta
}

// RenderText writes an ASCII list using go-pretty.
func (l *StringList) RenderText(w io.Writer) error {
	lw := list.NewWriter()
	lw.SetStyle(list.StyleBulletCircle)

	for _, item := range l.items {
		lw.AppendItem(item)
	}

	_, err := io.WriteString(w, lw.Render()+"\n")
	return err
}

// RenderJSON returns the items as an array of strings.
func (l *StringList) RenderJSON() any {
	return l.items
}

// RenderMarkdown writes a markdown list using go-pretty.
func (l *StringList) RenderMarkdown(w io.Writer) error {
	lw := list.NewWriter()
	lw.SetStyle(list.StyleMarkdown)

	for _, item := range l.items {
		lw.AppendItem(item)
	}

	_, err := io.WriteString(w, lw.RenderMarkdown()+"\n")
	return err
}
