package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// ParseFormat
// ---------------------------------------------------------------------------

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input string
		want  Format
	}{
		{"json", FormatJSON},
		{"markdown", FormatMarkdown},
		{"md", FormatMarkdown},
		{"text", FormatText},
		{"", FormatText},
		{"unknown", FormatText},
		{"JSON", FormatText}, // case-sensitive, uppercase not recognised
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseFormat(tt.input)
			if got != tt.want {
				t.Errorf("ParseFormat(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// NewMeta
// ---------------------------------------------------------------------------

func TestNewMeta(t *testing.T) {
	m := NewMeta("key-info")

	if m.Type != "key-info" {
		t.Errorf("Type = %q, want %q", m.Type, "key-info")
	}
	if m.Version != "v1" {
		t.Errorf("Version = %q, want %q", m.Version, "v1")
	}
	if m.Generated.IsZero() {
		t.Error("Generated should not be zero")
	}
}

// ---------------------------------------------------------------------------
// Meta.WithPagination
// ---------------------------------------------------------------------------

func TestMetaWithPagination(t *testing.T) {
	m := NewMeta("paged").WithPagination("abc123", true)

	if m.Cursor != "abc123" {
		t.Errorf("Cursor = %q, want %q", m.Cursor, "abc123")
	}
	if !m.HasMore {
		t.Error("HasMore should be true")
	}

	// Original is not mutated (value receiver).
	orig := NewMeta("paged")
	_ = orig.WithPagination("x", true)
	if orig.Cursor != "" {
		t.Error("WithPagination should not mutate original meta")
	}
}

// ---------------------------------------------------------------------------
// looksLikeHash
// ---------------------------------------------------------------------------

func TestLooksLikeHash(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"long hex lowercase", "abcdef0123456789abcdef0123456789", true},
		{"long hex uppercase", "ABCDEF0123456789ABCDEF0123456789", true},
		{"long hex mixed case", "AbCdEf0123456789AbCdEf0123456789", true},
		{"exactly 16 chars hex", "abcdef0123456789", true},
		{"short hex 15 chars", "abcdef012345678", false},
		{"short hex 8 chars", "abcd1234", false},
		{"non-hex characters", "xyz01234567890123456789", false},
		{"hex with spaces", "abcdef01 23456789", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := looksLikeHash(tt.s)
			if got != tt.want {
				t.Errorf("looksLikeHash(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// toJSONKey
// ---------------------------------------------------------------------------

func TestToJSONKey(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"My Header", "my_header"},
		{"Name", "name"},
		{"Public Key", "public_key"},
		{"already_lower", "already_lower"},
		{"UPPER CASE", "upper_case"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := toJSONKey(tt.input)
			if got != tt.want {
				t.Errorf("toJSONKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// KV
// ---------------------------------------------------------------------------

func TestKV_RenderText(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	kv := out.KV("key-info")
	kv.Set("Name", "alice")
	kv.Set("Public Key", "abcdef0123456789")

	if err := kv.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "Name:") {
		t.Errorf("text should contain 'Name:', got:\n%s", text)
	}
	if !strings.Contains(text, "alice") {
		t.Errorf("text should contain 'alice', got:\n%s", text)
	}
	if !strings.Contains(text, "Public Key:") {
		t.Errorf("text should contain 'Public Key:', got:\n%s", text)
	}
	if !strings.Contains(text, "abcdef0123456789") {
		t.Errorf("text should contain key value, got:\n%s", text)
	}
}

func TestKV_RenderJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	kv := out.KV("key-info")
	kv.Set("Name", "alice")
	kv.Set("Public Key", "abcdef0123456789")

	if err := kv.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Meta struct {
			Type    string `json:"type"`
			Version string `json:"version"`
		} `json:"meta"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v\nraw: %s", err, buf.String())
	}

	if envelope.Meta.Type != "key-info" {
		t.Errorf("meta.type = %q, want %q", envelope.Meta.Type, "key-info")
	}
	if envelope.Meta.Version != "v1" {
		t.Errorf("meta.version = %q, want %q", envelope.Meta.Version, "v1")
	}
	if envelope.Data["name"] != "alice" {
		t.Errorf("data.name = %v, want %q", envelope.Data["name"], "alice")
	}
	if envelope.Data["public_key"] != "abcdef0123456789" {
		t.Errorf("data.public_key = %v, want %q", envelope.Data["public_key"], "abcdef0123456789")
	}
}

func TestKV_RenderMarkdown(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	kv := out.KV("key-info")
	kv.Set("Name", "alice")
	kv.Set("Public Key", "abcdef0123456789abcdef0123456789")

	if err := kv.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	if !strings.Contains(md, "**Name:**") {
		t.Errorf("markdown should contain bold key '**Name:**', got:\n%s", md)
	}
	if !strings.Contains(md, "alice") {
		t.Errorf("markdown should contain 'alice', got:\n%s", md)
	}
	// Long hex should be wrapped in backticks.
	if !strings.Contains(md, "`abcdef0123456789abcdef0123456789`") {
		t.Errorf("markdown should wrap long hex in backticks, got:\n%s", md)
	}
	// Should have YAML frontmatter.
	if !strings.Contains(md, "---") {
		t.Errorf("markdown should contain frontmatter delimiters, got:\n%s", md)
	}
}

func TestKV_Empty(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	kv := out.KV("empty")
	if err := kv.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// Empty KV should produce no text output (the RenderText returns nil early).
	if buf.Len() != 0 {
		t.Errorf("empty KV text should produce no output, got %q", buf.String())
	}
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

func TestTable_RenderText(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	tbl := out.Table("key-list", "Name", "Public Key")
	tbl.AddRow("alice", "abc123")
	tbl.AddRow("bob", "def456")

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	upper := strings.ToUpper(text)
	if !strings.Contains(upper, "NAME") {
		t.Errorf("text should contain header 'Name' (any case), got:\n%s", text)
	}
	if !strings.Contains(text, "alice") {
		t.Errorf("text should contain 'alice', got:\n%s", text)
	}
	if !strings.Contains(text, "bob") {
		t.Errorf("text should contain 'bob', got:\n%s", text)
	}
}

func TestTable_RenderJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	tbl := out.Table("key-list", "Name", "Public Key")
	tbl.AddRow("alice", "abc123")
	tbl.AddRow("bob", "def456")

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Meta struct {
			Type string `json:"type"`
		} `json:"meta"`
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v\nraw: %s", err, buf.String())
	}

	if envelope.Meta.Type != "key-list" {
		t.Errorf("meta.type = %q, want %q", envelope.Meta.Type, "key-list")
	}
	if len(envelope.Data) != 2 {
		t.Fatalf("data length = %d, want 2", len(envelope.Data))
	}
	if envelope.Data[0]["name"] != "alice" {
		t.Errorf("data[0].name = %q, want %q", envelope.Data[0]["name"], "alice")
	}
	if envelope.Data[0]["public_key"] != "abc123" {
		t.Errorf("data[0].public_key = %q, want %q", envelope.Data[0]["public_key"], "abc123")
	}
	if envelope.Data[1]["name"] != "bob" {
		t.Errorf("data[1].name = %q, want %q", envelope.Data[1]["name"], "bob")
	}
}

func TestTable_RenderMarkdown(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	tbl := out.Table("key-list", "Name", "Public Key")
	tbl.AddRow("alice", "abc123")

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	// Markdown table uses | delimiters.
	if !strings.Contains(md, "| Name") {
		t.Errorf("markdown should contain '| Name' header, got:\n%s", md)
	}
	if !strings.Contains(md, "alice") {
		t.Errorf("markdown should contain 'alice', got:\n%s", md)
	}
	// Should have separator row.
	if !strings.Contains(md, "---") {
		t.Errorf("markdown should contain separator row, got:\n%s", md)
	}
}

func TestTable_EmptyRows(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	tbl := out.Table("empty-table", "Name")

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}
	if len(envelope.Data) != 0 {
		t.Errorf("data length = %d, want 0", len(envelope.Data))
	}
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

func TestResult_RenderText(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	res := out.Result("send-result", "Message sent successfully")
	res.With("Recipient", "bob")
	res.With("Size", 42)

	if err := res.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "Message sent successfully") {
		t.Errorf("text should contain message, got:\n%s", text)
	}
	if !strings.Contains(text, "Recipient") {
		t.Errorf("text should contain detail key 'Recipient', got:\n%s", text)
	}
	if !strings.Contains(text, "bob") {
		t.Errorf("text should contain detail value 'bob', got:\n%s", text)
	}
}

func TestResult_RenderJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	res := out.Result("send-result", "Message sent successfully")
	res.With("Recipient", "bob")

	if err := res.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Meta struct {
			Type string `json:"type"`
		} `json:"meta"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v\nraw: %s", err, buf.String())
	}

	if envelope.Meta.Type != "send-result" {
		t.Errorf("meta.type = %q, want %q", envelope.Meta.Type, "send-result")
	}
	if envelope.Data["message"] != "Message sent successfully" {
		t.Errorf("data.message = %v, want %q", envelope.Data["message"], "Message sent successfully")
	}
	if envelope.Data["recipient"] != "bob" {
		t.Errorf("data.recipient = %v, want %q", envelope.Data["recipient"], "bob")
	}
}

func TestResult_RenderMarkdown(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	res := out.Result("send-result", "Message sent successfully")
	res.With("Recipient", "bob")

	if err := res.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	if !strings.Contains(md, "**Message sent successfully**") {
		t.Errorf("markdown should contain bold message, got:\n%s", md)
	}
	if !strings.Contains(md, "**Recipient:**") {
		t.Errorf("markdown should contain bold detail key, got:\n%s", md)
	}
}

func TestResult_NoDetails(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	res := out.Result("simple", "Done")

	if err := res.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "Done") {
		t.Errorf("text should contain 'Done', got:\n%s", text)
	}
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

func TestError_RenderText_WithCode(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	e := out.Error("send", errors.New("connection refused"))
	e.WithCode("RELAY_UNAVAILABLE")

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "Error [RELAY_UNAVAILABLE]") {
		t.Errorf("text should contain error code, got:\n%s", text)
	}
	if !strings.Contains(text, "connection refused") {
		t.Errorf("text should contain error message, got:\n%s", text)
	}
}

func TestError_RenderText_WithoutCode(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	e := out.Error("send", errors.New("something broke"))

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "Error:") {
		t.Errorf("text should contain 'Error:', got:\n%s", text)
	}
	if !strings.Contains(text, "something broke") {
		t.Errorf("text should contain error message, got:\n%s", text)
	}
	// Should NOT contain brackets when no code is set.
	if strings.Contains(text, "[") {
		t.Errorf("text should not contain brackets without code, got:\n%s", text)
	}
}

func TestError_RenderText_WithDetails(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	e := out.Error("send", errors.New("failed"))
	e.With("relay", "localhost:9090")

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "relay") {
		t.Errorf("text should contain detail key 'relay', got:\n%s", text)
	}
	if !strings.Contains(text, "localhost:9090") {
		t.Errorf("text should contain detail value, got:\n%s", text)
	}
}

func TestError_RenderJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	e := out.Error("send", errors.New("connection refused"))
	e.WithCode("RELAY_UNAVAILABLE")
	e.With("Relay", "localhost:9090")

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Meta struct {
			Type string `json:"type"`
		} `json:"meta"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v\nraw: %s", err, buf.String())
	}

	// Error type has "-error" appended.
	if envelope.Meta.Type != "send-error" {
		t.Errorf("meta.type = %q, want %q", envelope.Meta.Type, "send-error")
	}
	if envelope.Data["error"] != "connection refused" {
		t.Errorf("data.error = %v, want %q", envelope.Data["error"], "connection refused")
	}
	if envelope.Data["code"] != "RELAY_UNAVAILABLE" {
		t.Errorf("data.code = %v, want %q", envelope.Data["code"], "RELAY_UNAVAILABLE")
	}
	if envelope.Data["relay"] != "localhost:9090" {
		t.Errorf("data.relay = %v, want %q", envelope.Data["relay"], "localhost:9090")
	}
}

func TestError_RenderJSON_WithoutCode(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	e := out.Error("send", errors.New("timeout"))

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if envelope.Data["error"] != "timeout" {
		t.Errorf("data.error = %v, want %q", envelope.Data["error"], "timeout")
	}
	// Code should be absent.
	if _, ok := envelope.Data["code"]; ok {
		t.Error("data.code should not be present when no code is set")
	}
}

func TestError_RenderMarkdown(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	e := out.Error("send", errors.New("connection refused"))
	e.WithCode("RELAY_UNAVAILABLE")

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	if !strings.Contains(md, "**Error [RELAY_UNAVAILABLE]:**") {
		t.Errorf("markdown should contain bold error with code, got:\n%s", md)
	}
	if !strings.Contains(md, "connection refused") {
		t.Errorf("markdown should contain error message, got:\n%s", md)
	}
	// Markdown errors use blockquote.
	if !strings.Contains(md, ">") {
		t.Errorf("markdown error should use blockquote, got:\n%s", md)
	}
}

func TestError_RenderMarkdown_WithoutCode(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	e := out.Error("send", errors.New("oops"))

	if err := e.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	if !strings.Contains(md, "> **Error:**") {
		t.Errorf("markdown should contain '> **Error:**', got:\n%s", md)
	}
}

// ---------------------------------------------------------------------------
// StringList
// ---------------------------------------------------------------------------

func TestStringList_RenderText(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	sl := out.StringList("aliases")
	sl.Add("alice", "bob", "carol")

	if err := sl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "alice") {
		t.Errorf("text should contain 'alice', got:\n%s", text)
	}
	if !strings.Contains(text, "bob") {
		t.Errorf("text should contain 'bob', got:\n%s", text)
	}
	if !strings.Contains(text, "carol") {
		t.Errorf("text should contain 'carol', got:\n%s", text)
	}
}

func TestStringList_RenderJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	sl := out.StringList("aliases")
	sl.Add("alice", "bob")

	if err := sl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Meta struct {
			Type string `json:"type"`
		} `json:"meta"`
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v\nraw: %s", err, buf.String())
	}

	if envelope.Meta.Type != "aliases" {
		t.Errorf("meta.type = %q, want %q", envelope.Meta.Type, "aliases")
	}
	if len(envelope.Data) != 2 {
		t.Fatalf("data length = %d, want 2", len(envelope.Data))
	}
	if envelope.Data[0] != "alice" {
		t.Errorf("data[0] = %q, want %q", envelope.Data[0], "alice")
	}
	if envelope.Data[1] != "bob" {
		t.Errorf("data[1] = %q, want %q", envelope.Data[1], "bob")
	}
}

func TestStringList_RenderMarkdown(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	sl := out.StringList("aliases")
	sl.Add("alice", "bob")

	if err := sl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	if !strings.Contains(md, "alice") {
		t.Errorf("markdown should contain 'alice', got:\n%s", md)
	}
	if !strings.Contains(md, "bob") {
		t.Errorf("markdown should contain 'bob', got:\n%s", md)
	}
	// Markdown list should contain frontmatter.
	if !strings.Contains(md, "---") {
		t.Errorf("markdown should contain frontmatter, got:\n%s", md)
	}
}

// ---------------------------------------------------------------------------
// Output.Render dispatches to correct format
// ---------------------------------------------------------------------------

func TestOutputRender_DispatchesText(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	res := out.Result("test", "hello text")
	if err := out.Render(res); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "hello text") {
		t.Errorf("text output missing message, got:\n%s", text)
	}
	// Text output should NOT contain JSON envelope.
	if strings.Contains(text, `"meta"`) {
		t.Errorf("text output should not contain JSON envelope, got:\n%s", text)
	}
}

func TestOutputRender_DispatchesJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	res := out.Result("test", "hello json")
	if err := out.Render(res); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	// Must be valid JSON.
	var envelope map[string]any
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("output is not valid JSON: %v\nraw: %s", err, buf.String())
	}
	if _, ok := envelope["meta"]; !ok {
		t.Error("JSON output should have 'meta' key")
	}
	if _, ok := envelope["data"]; !ok {
		t.Error("JSON output should have 'data' key")
	}
}

func TestOutputRender_DispatchesMarkdown(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatMarkdown, &buf)

	res := out.Result("test", "hello markdown")
	if err := out.Render(res); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	md := buf.String()
	// Should have YAML frontmatter.
	if !strings.HasPrefix(md, "---") {
		t.Errorf("markdown output should start with frontmatter, got:\n%s", md)
	}
	if !strings.Contains(md, "**hello markdown**") {
		t.Errorf("markdown output should contain bold message, got:\n%s", md)
	}
}

// ---------------------------------------------------------------------------
// Pagination hint in text output
// ---------------------------------------------------------------------------

func TestOutputRender_PaginationHint_Text(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	tbl := out.Table("key-list", "Name")
	tbl.AddRow("alice")
	tbl.WithPagination("cursor-abc", true)

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "More results: --cursor=cursor-abc") {
		t.Errorf("text should contain pagination hint, got:\n%s", text)
	}
}

func TestOutputRender_NoPaginationHint_WhenNoMore(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	tbl := out.Table("key-list", "Name")
	tbl.AddRow("alice")
	tbl.WithPagination("cursor-abc", false)

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if strings.Contains(text, "More results") {
		t.Errorf("text should NOT contain pagination hint when hasMore=false, got:\n%s", text)
	}
}

func TestOutputRender_NoPaginationHint_WhenNoCursor(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	tbl := out.Table("key-list", "Name")
	tbl.AddRow("alice")
	// HasMore is true but no cursor set (default empty).

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if strings.Contains(text, "More results") {
		t.Errorf("text should NOT contain pagination hint when cursor is empty, got:\n%s", text)
	}
}

func TestOutputRender_PaginationInJSON(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	tbl := out.Table("key-list", "Name")
	tbl.AddRow("alice")
	tbl.WithPagination("next-page", true)

	if err := tbl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Meta struct {
			Cursor  string `json:"cursor"`
			HasMore bool   `json:"has_more"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if envelope.Meta.Cursor != "next-page" {
		t.Errorf("meta.cursor = %q, want %q", envelope.Meta.Cursor, "next-page")
	}
	if !envelope.Meta.HasMore {
		t.Error("meta.has_more should be true")
	}
}

// ---------------------------------------------------------------------------
// KV with pagination
// ---------------------------------------------------------------------------

func TestKV_WithPagination(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	kv := out.KV("test")
	kv.Set("key", "value")
	kv.WithPagination("kv-cursor", true)

	if err := kv.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "More results: --cursor=kv-cursor") {
		t.Errorf("text should contain pagination hint, got:\n%s", text)
	}
}

// ---------------------------------------------------------------------------
// StringList with pagination
// ---------------------------------------------------------------------------

func TestStringList_WithPagination(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatText, &buf)

	sl := out.StringList("items")
	sl.Add("first")
	sl.WithPagination("sl-cursor", true)

	if err := sl.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	text := buf.String()
	if !strings.Contains(text, "More results: --cursor=sl-cursor") {
		t.Errorf("text should contain pagination hint, got:\n%s", text)
	}
}

// ---------------------------------------------------------------------------
// Output.Format returns configured format
// ---------------------------------------------------------------------------

func TestOutput_Format(t *testing.T) {
	tests := []struct {
		format Format
	}{
		{FormatText},
		{FormatJSON},
		{FormatMarkdown},
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			var buf bytes.Buffer
			out := NewOutput(tt.format, &buf)
			if got := out.Format(); got != tt.format {
				t.Errorf("Format() = %q, want %q", got, tt.format)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// JSON envelope structure validation
// ---------------------------------------------------------------------------

func TestJSON_EnvelopeStructure(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	res := out.Result("envelope-test", "checking structure")
	if err := res.Render(); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	// Must have exactly "meta" and "data" top-level keys.
	if len(raw) != 2 {
		t.Errorf("envelope should have 2 top-level keys, got %d", len(raw))
	}
	if _, ok := raw["meta"]; !ok {
		t.Error("envelope missing 'meta' key")
	}
	if _, ok := raw["data"]; !ok {
		t.Error("envelope missing 'data' key")
	}

	// Validate meta has expected fields.
	var meta struct {
		Type      string `json:"type"`
		Version   string `json:"version"`
		Generated string `json:"generated"`
	}
	if err := json.Unmarshal(raw["meta"], &meta); err != nil {
		t.Fatalf("meta unmarshal error = %v", err)
	}
	if meta.Type != "envelope-test" {
		t.Errorf("meta.type = %q, want %q", meta.Type, "envelope-test")
	}
	if meta.Version != "v1" {
		t.Errorf("meta.version = %q, want %q", meta.Version, "v1")
	}
	if meta.Generated == "" {
		t.Error("meta.generated should not be empty")
	}
}

// ---------------------------------------------------------------------------
// formatMarkdownValue
// ---------------------------------------------------------------------------

func TestFormatMarkdownValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{
			name: "plain string",
			val:  "hello",
			want: "hello",
		},
		{
			name: "hash value gets backticks",
			val:  "abcdef0123456789abcdef0123456789",
			want: "`abcdef0123456789abcdef0123456789`",
		},
		{
			name: "pipe gets escaped",
			val:  "a|b",
			want: "a\\|b",
		},
		{
			name: "short string no backticks",
			val:  "abc123",
			want: "abc123",
		},
		{
			name: "integer value",
			val:  42,
			want: "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatMarkdownValue(tt.val)
			if got != tt.want {
				t.Errorf("formatMarkdownValue(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Method chaining
// ---------------------------------------------------------------------------

func TestKV_Chaining(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	// All Set calls should return *KV for chaining.
	err := out.KV("chain-test").
		Set("a", 1).
		Set("b", 2).
		Set("c", 3).
		Render()

	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if len(envelope.Data) != 3 {
		t.Errorf("data should have 3 keys, got %d", len(envelope.Data))
	}
}

func TestTable_Chaining(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	err := out.Table("chain-test", "X", "Y").
		AddRow("1", "2").
		AddRow("3", "4").
		Render()

	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if len(envelope.Data) != 2 {
		t.Errorf("data should have 2 rows, got %d", len(envelope.Data))
	}
}

func TestResult_Chaining(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	err := out.Result("chain-test", "ok").
		With("a", 1).
		With("b", "two").
		Render()

	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if envelope.Data["message"] != "ok" {
		t.Errorf("data.message = %v, want %q", envelope.Data["message"], "ok")
	}
}

func TestError_Chaining(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	err := out.Error("chain-test", errors.New("fail")).
		WithCode("TEST").
		With("ctx", "info").
		Render()

	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if envelope.Data["error"] != "fail" {
		t.Errorf("data.error = %v, want %q", envelope.Data["error"], "fail")
	}
	if envelope.Data["code"] != "TEST" {
		t.Errorf("data.code = %v, want %q", envelope.Data["code"], "TEST")
	}
}

func TestStringList_Chaining(t *testing.T) {
	var buf bytes.Buffer
	out := NewOutput(FormatJSON, &buf)

	err := out.StringList("chain-test").
		Add("a", "b").
		Add("c").
		Render()

	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var envelope struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("JSON unmarshal error = %v", err)
	}

	if len(envelope.Data) != 3 {
		t.Errorf("data should have 3 items, got %d", len(envelope.Data))
	}
}
