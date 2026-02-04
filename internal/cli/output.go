package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Format represents an output format.
type Format string

const (
	FormatText     Format = "text"
	FormatJSON     Format = "json"
	FormatMarkdown Format = "markdown"
)

// ParseFormat parses a format string, defaulting to text.
func ParseFormat(s string) Format {
	switch s {
	case "json":
		return FormatJSON
	case "markdown", "md":
		return FormatMarkdown
	default:
		return FormatText
	}
}

// Meta contains metadata for progressive disclosure and pagination.
type Meta struct {
	Type      string    `json:"type" yaml:"type"`
	Version   string    `json:"version,omitempty" yaml:"version,omitempty"`
	Generated time.Time `json:"generated" yaml:"generated"`
	Cursor    string    `json:"cursor,omitempty" yaml:"cursor,omitempty"`
	HasMore   bool      `json:"has_more,omitempty" yaml:"has_more,omitempty"`
}

// NewMeta creates metadata with the given type and current timestamp.
func NewMeta(resultType string) Meta {
	return Meta{
		Type:      resultType,
		Version:   "v1",
		Generated: time.Now().UTC(),
	}
}

// WithPagination adds pagination info to metadata.
func (m Meta) WithPagination(cursor string, hasMore bool) Meta {
	m.Cursor = cursor
	m.HasMore = hasMore
	return m
}

// Renderable can render itself in multiple formats.
type Renderable interface {
	Meta() Meta
	RenderText(w io.Writer) error
	RenderJSON() any
	RenderMarkdown(w io.Writer) error
}

// Output handles formatted rendering with automatic envelope/frontmatter.
type Output struct {
	format Format
	w      io.Writer
}

// NewOutput creates an output renderer for the given format.
func NewOutput(format Format, w io.Writer) *Output {
	return &Output{format: format, w: w}
}

// NewOutputFromViper creates an output renderer from viper config.
// Reads the "output" key for format (text, json, markdown).
func NewOutputFromViper(v ViperGetter) *Output {
	format := ParseFormat(v.GetString("output"))
	return NewOutput(format, os.Stdout)
}

// ViperGetter is the subset of viper.Viper we need.
type ViperGetter interface {
	GetString(key string) string
}

// Format returns the configured output format.
func (o *Output) Format() Format {
	return o.format
}

// Table creates a new table renderer attached to this output.
func (o *Output) Table(resultType string, headers ...string) *Table {
	return &Table{
		out:     o,
		meta:    NewMeta(resultType),
		headers: headers,
	}
}

// KV creates a new key-value renderer attached to this output.
func (o *Output) KV(resultType string) *KV {
	return &KV{
		out:  o,
		meta: NewMeta(resultType),
	}
}

// List creates a new list renderer attached to this output.
func (o *Output) List(resultType string) *List {
	return &List{
		out:  o,
		meta: NewMeta(resultType),
	}
}

// StringList creates a new string list renderer attached to this output.
func (o *Output) StringList(resultType string) *StringList {
	return &StringList{
		out:  o,
		meta: NewMeta(resultType),
	}
}

// Result creates a new result renderer attached to this output.
func (o *Output) Result(resultType, message string) *Result {
	return &Result{
		out:     o,
		meta:    NewMeta(resultType),
		message: message,
		details: make(map[string]any),
	}
}

// Error creates a new error renderer attached to this output.
func (o *Output) Error(resultType string, err error) *Error {
	return &Error{
		out:     o,
		meta:    NewMeta(resultType + "-error"),
		err:     err,
		details: make(map[string]any),
	}
}

// Render outputs the renderable in the configured format.
func (o *Output) Render(r Renderable) error {
	switch o.format {
	case FormatJSON:
		return o.renderJSON(r)
	case FormatMarkdown:
		return o.renderMarkdown(r)
	default:
		return o.renderText(r)
	}
}

func (o *Output) renderText(r Renderable) error {
	if err := r.RenderText(o.w); err != nil {
		return err
	}

	// Show pagination hint if there's more data
	meta := r.Meta()
	if meta.HasMore && meta.Cursor != "" {
		if _, err := fmt.Fprintf(o.w, "\nMore results: --cursor=%s\n", meta.Cursor); err != nil {
			return err
		}
	}

	return nil
}

func (o *Output) renderJSON(r Renderable) error {
	envelope := struct {
		Meta Meta `json:"meta"`
		Data any  `json:"data"`
	}{
		Meta: r.Meta(),
		Data: r.RenderJSON(),
	}

	enc := json.NewEncoder(o.w)
	enc.SetIndent("", "  ")
	return enc.Encode(envelope)
}

func (o *Output) renderMarkdown(r Renderable) error {
	// Write YAML frontmatter
	meta := r.Meta()
	if _, err := fmt.Fprintln(o.w, "---"); err != nil {
		return err
	}

	enc := yaml.NewEncoder(o.w)
	enc.SetIndent(2)
	if err := enc.Encode(meta); err != nil {
		return err
	}
	if err := enc.Close(); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(o.w, "---"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(o.w); err != nil {
		return err
	}

	return r.RenderMarkdown(o.w)
}
