package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
)

// KV renders key-value pairs using go-pretty table.
// Created via Output.KV().
type KV struct {
	out   *Output
	meta  Meta
	pairs []kvPair
}

type kvPair struct {
	key   string
	value any
}

// Set adds a key-value pair. Value can be any type.
func (k *KV) Set(key string, value any) *KV {
	k.pairs = append(k.pairs, kvPair{key: key, value: value})
	return k
}

// WithPagination sets pagination cursor and hasMore flag.
func (k *KV) WithPagination(cursor string, hasMore bool) *KV {
	k.meta = k.meta.WithPagination(cursor, hasMore)
	return k
}

// Render outputs the key-value pairs in the configured format.
func (k *KV) Render() error {
	return k.out.Render(k)
}

// Meta returns the metadata.
func (k *KV) Meta() Meta {
	return k.meta
}

// RenderText writes aligned key: value pairs using go-pretty.
func (k *KV) RenderText(w io.Writer) error {
	if len(k.pairs) == 0 {
		return nil
	}

	tw := table.NewWriter()
	tw.SetStyle(table.StyleLight)
	tw.Style().Options.DrawBorder = false
	tw.Style().Options.SeparateColumns = false
	tw.Style().Options.SeparateRows = false
	tw.Style().Options.SeparateHeader = false

	for _, p := range k.pairs {
		tw.AppendRow(table.Row{p.key + ":", fmt.Sprintf("%v", p.value)})
	}

	_, err := io.WriteString(w, tw.Render()+"\n")
	return err
}

// RenderJSON returns the data as an object.
func (k *KV) RenderJSON() any {
	result := make(map[string]any, len(k.pairs))
	for _, p := range k.pairs {
		result[toJSONKey(p.key)] = p.value
	}
	return result
}

// RenderMarkdown writes key-value pairs as a definition-style list.
func (k *KV) RenderMarkdown(w io.Writer) error {
	for _, p := range k.pairs {
		value := formatMarkdownValue(p.value)
		if _, err := fmt.Fprintf(w, "**%s:** %s\n\n", p.key, value); err != nil {
			return err
		}
	}
	return nil
}

// formatMarkdownValue formats a value for markdown output.
func formatMarkdownValue(v any) string {
	s := fmt.Sprintf("%v", v)

	// Wrap hex hashes in backticks
	if looksLikeHash(s) {
		return "`" + s + "`"
	}

	// Escape special markdown characters
	s = strings.ReplaceAll(s, "|", "\\|")

	return s
}

// looksLikeHash returns true if the string looks like a hex hash or key.
func looksLikeHash(s string) bool {
	if len(s) < 16 {
		return false
	}
	for _, c := range s {
		isDigit := c >= '0' && c <= '9'
		isLowerHex := c >= 'a' && c <= 'f'
		isUpperHex := c >= 'A' && c <= 'F'
		if !isDigit && !isLowerHex && !isUpperHex {
			return false
		}
	}
	return true
}
