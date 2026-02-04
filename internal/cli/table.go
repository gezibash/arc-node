package cli

import (
	"io"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
)

// Table renders tabular data using go-pretty.
// Created via Output.Table().
type Table struct {
	out     *Output
	meta    Meta
	headers []string
	rows    [][]string
}

// AddRow adds a row of values. Should match header count.
func (t *Table) AddRow(values ...string) *Table {
	t.rows = append(t.rows, values)
	return t
}

// WithPagination sets pagination cursor and hasMore flag.
func (t *Table) WithPagination(cursor string, hasMore bool) *Table {
	t.meta = t.meta.WithPagination(cursor, hasMore)
	return t
}

// Render outputs the table in the configured format.
func (t *Table) Render() error {
	return t.out.Render(t)
}

// Meta returns the table metadata.
func (t *Table) Meta() Meta {
	return t.meta
}

// RenderText writes an ASCII table using go-pretty.
func (t *Table) RenderText(w io.Writer) error {
	tw := t.newTableWriter()
	tw.SetStyle(table.StyleLight)
	_, err := io.WriteString(w, tw.Render()+"\n")
	return err
}

// RenderJSON returns the data as an array of objects.
func (t *Table) RenderJSON() any {
	result := make([]map[string]string, 0, len(t.rows))
	for _, row := range t.rows {
		obj := make(map[string]string)
		for i, h := range t.headers {
			if i < len(row) {
				obj[toJSONKey(h)] = row[i]
			}
		}
		result = append(result, obj)
	}
	return result
}

// RenderMarkdown writes a markdown table using go-pretty.
func (t *Table) RenderMarkdown(w io.Writer) error {
	tw := t.newTableWriter()
	_, err := io.WriteString(w, tw.RenderMarkdown()+"\n")
	return err
}

// newTableWriter creates a configured go-pretty table writer.
func (t *Table) newTableWriter() table.Writer {
	tw := table.NewWriter()

	// Add header
	headerRow := make(table.Row, len(t.headers))
	for i, h := range t.headers {
		headerRow[i] = h
	}
	tw.AppendHeader(headerRow)

	// Add rows
	for _, row := range t.rows {
		tableRow := make(table.Row, len(row))
		for i, cell := range row {
			tableRow[i] = cell
		}
		tw.AppendRow(tableRow)
	}

	return tw
}

// toJSONKey converts a header to a JSON key (lowercase, underscores).
func toJSONKey(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, " ", "_"))
}
