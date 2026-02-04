package cli

import (
	"fmt"
	"io"
)

// Result is a simple single-message result.
// Created via Output.Result().
type Result struct {
	out     *Output
	meta    Meta
	message string
	details map[string]any
}

// With adds a detail key-value pair.
func (r *Result) With(key string, value any) *Result {
	r.details[key] = value
	return r
}

// Render outputs the result in the configured format.
func (r *Result) Render() error {
	return r.out.Render(r)
}

// Meta returns the metadata.
func (r *Result) Meta() Meta {
	return r.meta
}

// RenderText writes the message and details.
func (r *Result) RenderText(w io.Writer) error {
	if _, err := fmt.Fprintln(w, r.message); err != nil {
		return err
	}

	if len(r.details) > 0 {
		// Find max key length
		maxLen := 0
		for k := range r.details {
			if len(k) > maxLen {
				maxLen = len(k)
			}
		}

		for k, v := range r.details {
			if _, err := fmt.Fprintf(w, "  %-*s  %v\n", maxLen, k+":", v); err != nil {
				return err
			}
		}
	}

	return nil
}

// RenderJSON returns message and details as object.
func (r *Result) RenderJSON() any {
	result := make(map[string]any, len(r.details)+1)
	result["message"] = r.message
	for k, v := range r.details {
		result[toJSONKey(k)] = v
	}
	return result
}

// RenderMarkdown writes the result in markdown.
func (r *Result) RenderMarkdown(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "**%s**\n\n", r.message); err != nil {
		return err
	}

	if len(r.details) > 0 {
		for k, v := range r.details {
			if _, err := fmt.Fprintf(w, "- **%s:** %v\n", k, formatMarkdownValue(v)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Error is a structured error result.
// Created via Output.Error().
type Error struct {
	out     *Output
	meta    Meta
	err     error
	code    string
	details map[string]any
}

// WithCode sets an error code.
func (e *Error) WithCode(code string) *Error {
	e.code = code
	return e
}

// With adds a detail key-value pair.
func (e *Error) With(key string, value any) *Error {
	e.details[key] = value
	return e
}

// Render outputs the error in the configured format.
func (e *Error) Render() error {
	return e.out.Render(e)
}

// Meta returns the metadata.
func (e *Error) Meta() Meta {
	return e.meta
}

// RenderText writes the error.
func (e *Error) RenderText(w io.Writer) error {
	if e.code != "" {
		if _, err := fmt.Fprintf(w, "Error [%s]: %v\n", e.code, e.err); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(w, "Error: %v\n", e.err); err != nil {
			return err
		}
	}

	for k, v := range e.details {
		if _, err := fmt.Fprintf(w, "  %s: %v\n", k, v); err != nil {
			return err
		}
	}

	return nil
}

// RenderJSON returns error as object.
func (e *Error) RenderJSON() any {
	result := map[string]any{
		"error": e.err.Error(),
	}
	if e.code != "" {
		result["code"] = e.code
	}
	for k, v := range e.details {
		result[toJSONKey(k)] = v
	}
	return result
}

// RenderMarkdown writes the error in markdown.
func (e *Error) RenderMarkdown(w io.Writer) error {
	if e.code != "" {
		if _, err := fmt.Fprintf(w, "> **Error [%s]:** %v\n", e.code, e.err); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(w, "> **Error:** %v\n", e.err); err != nil {
			return err
		}
	}

	if len(e.details) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		for k, v := range e.details {
			if _, err := fmt.Fprintf(w, "- %s: %v\n", k, v); err != nil {
				return err
			}
		}
	}

	return nil
}
