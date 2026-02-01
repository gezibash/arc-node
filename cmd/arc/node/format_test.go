package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/pkg/reference"
)

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string {
	return ansiRe.ReplaceAllString(s, "")
}

func testEntry(labels map[string]string) *client.Entry {
	ref := reference.Compute([]byte("test"))
	return &client.Entry{
		Ref:       ref,
		Labels:    labels,
		Timestamp: time.Now().Add(-5 * time.Second).UnixMilli(),
	}
}

func TestFormatEntryText(t *testing.T) {
	e := testEntry(map[string]string{"env": "prod", "app": "arc"})
	var buf bytes.Buffer
	if err := formatEntryText(&buf, e, false, nil, nil); err != nil {
		t.Fatalf("formatEntryText: %v", err)
	}
	out := stripANSI(buf.String())

	refHex := reference.Hex(e.Ref)
	short := refHex[:8]
	if !strings.Contains(out, short) {
		t.Errorf("output missing ref: %s", out)
	}
	if !strings.Contains(out, "ago") {
		t.Errorf("output missing relative time: %s", out)
	}
	if !strings.Contains(out, "env=prod") {
		t.Errorf("output missing label env=prod: %s", out)
	}
	if !strings.Contains(out, "app=arc") {
		t.Errorf("output missing label app=arc: %s", out)
	}
	if !strings.HasSuffix(out, "\n\n") {
		t.Errorf("output missing blank-line separator: %q", out)
	}
}

func TestFormatEntryTextNoLabels(t *testing.T) {
	e := testEntry(nil)
	var buf bytes.Buffer
	if err := formatEntryText(&buf, e, false, nil, nil); err != nil {
		t.Fatalf("formatEntryText: %v", err)
	}
	out := stripANSI(buf.String())

	refHex := reference.Hex(e.Ref)
	short := refHex[:8]
	if !strings.Contains(out, short) {
		t.Errorf("output missing ref: %s", out)
	}
	if !strings.Contains(out, "ago") {
		t.Errorf("output missing relative time: %s", out)
	}
	// Should only have the ref line + blank line (no labels line)
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 1 {
		t.Errorf("expected 1 content line, got %d: %q", len(lines), out)
	}
	if !strings.HasSuffix(out, "\n\n") {
		t.Errorf("output missing blank-line separator: %q", out)
	}
}

func TestFormatEntryJSON(t *testing.T) {
	e := testEntry(map[string]string{"env": "prod"})
	var buf bytes.Buffer
	if err := formatEntryJSON(&buf, e, false, nil, nil); err != nil {
		t.Fatalf("formatEntryJSON: %v", err)
	}

	var parsed jsonEntry
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, buf.String())
	}

	fullHex := reference.Hex(e.Ref)
	if parsed.Reference != fullHex {
		t.Errorf("reference = %q, want %q", parsed.Reference, fullHex)
	}
	if parsed.Labels["env"] != "prod" {
		t.Errorf("labels = %v", parsed.Labels)
	}
	if parsed.Timestamp != e.Timestamp {
		t.Errorf("timestamp = %d, want %d", parsed.Timestamp, e.Timestamp)
	}
}

func TestFormatEntryJSONNoLabels(t *testing.T) {
	e := testEntry(nil)
	var buf bytes.Buffer
	if err := formatEntryJSON(&buf, e, false, nil, nil); err != nil {
		t.Fatalf("formatEntryJSON: %v", err)
	}

	// labels should be omitted (omitempty)
	if strings.Contains(buf.String(), `"labels"`) {
		t.Errorf("expected labels to be omitted, got: %s", buf.String())
	}
}

func TestFormatEntryDispatch(t *testing.T) {
	e := testEntry(map[string]string{"k": "v"})

	var jsonBuf bytes.Buffer
	if err := formatEntry(&jsonBuf, e, "json", false, nil, nil); err != nil {
		t.Fatalf("formatEntry json: %v", err)
	}
	var parsed jsonEntry
	if err := json.Unmarshal(jsonBuf.Bytes(), &parsed); err != nil {
		t.Fatalf("json dispatch did not produce valid JSON: %v", err)
	}

	var textBuf bytes.Buffer
	if err := formatEntry(&textBuf, e, "text", false, nil, nil); err != nil {
		t.Fatalf("formatEntry text: %v", err)
	}
	if !strings.Contains(stripANSI(textBuf.String()), "ago") {
		t.Errorf("text dispatch missing 'ago': %s", textBuf.String())
	}
}

func testPreviewEntry(contentBody []byte) (*client.Entry, ContentLoader) {
	contentRef := reference.Compute(contentBody)
	contentHex := reference.Hex(contentRef)

	e := &client.Entry{
		Ref:       reference.Compute([]byte("msg")),
		Labels:    map[string]string{"content": contentHex},
		Timestamp: time.Now().Add(-2 * time.Second).UnixMilli(),
	}

	loader := func(_ context.Context, ref reference.Reference) ([]byte, error) {
		if ref == contentRef {
			return contentBody, nil
		}
		return nil, errors.New("unknown ref")
	}

	return e, loader
}

func TestFormatEntryTextPreview(t *testing.T) {
	e, loader := testPreviewEntry([]byte("Hello, this is preview content\nline two\n"))

	var buf bytes.Buffer
	if err := formatEntryText(&buf, e, true, context.Background(), loader); err != nil {
		t.Fatalf("formatEntryText with preview: %v", err)
	}
	out := stripANSI(buf.String())

	if !strings.Contains(out, "  Hello, this is preview content") {
		t.Errorf("output missing indented preview content: %s", out)
	}
	if !strings.Contains(out, "  line two") {
		t.Errorf("output missing second line: %s", out)
	}
}

func TestFormatEntryTextPreviewTruncateLines(t *testing.T) {
	e, loader := testPreviewEntry([]byte("line1\nline2\nline3\nline4\nline5\nline6\n"))

	var buf bytes.Buffer
	if err := formatEntryText(&buf, e, true, context.Background(), loader); err != nil {
		t.Fatalf("formatEntryText: %v", err)
	}
	out := stripANSI(buf.String())

	if !strings.Contains(out, "...") {
		t.Errorf("expected truncation suffix: %s", out)
	}
	if strings.Contains(out, "line5") {
		t.Errorf("should not contain line5 after truncation: %s", out)
	}
}

func TestFormatEntryTextPreviewError(t *testing.T) {
	e := testEntry(nil)

	loader := func(_ context.Context, ref reference.Reference) ([]byte, error) {
		return nil, errors.New("not found")
	}

	var buf bytes.Buffer
	if err := formatEntryText(&buf, e, true, context.Background(), loader); err != nil {
		t.Fatalf("formatEntryText should not fail on preview error: %v", err)
	}
	// With the new format, failed previews are silently omitted
}

func TestFormatEntryJSONPreview(t *testing.T) {
	e, loader := testPreviewEntry([]byte("json preview content"))

	var buf bytes.Buffer
	if err := formatEntryJSON(&buf, e, true, context.Background(), loader); err != nil {
		t.Fatalf("formatEntryJSON with preview: %v", err)
	}

	var parsed jsonEntry
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if parsed.Preview != "json preview content" {
		t.Errorf("preview = %q, want %q", parsed.Preview, "json preview content")
	}
}

func TestTruncatePreview(t *testing.T) {
	t.Run("short content unchanged", func(t *testing.T) {
		got := truncatePreview([]byte("hello"))
		if got != "hello" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("byte truncation", func(t *testing.T) {
		data := bytes.Repeat([]byte("a"), 300)
		got := truncatePreview(data)
		if len(got) > 260 { // 256 + "..."
			t.Errorf("too long: %d", len(got))
		}
		if !strings.HasSuffix(got, "...") {
			t.Errorf("missing suffix: %q", got)
		}
	})

	t.Run("line truncation", func(t *testing.T) {
		got := truncatePreview([]byte("1\n2\n3\n4\n5\n"))
		if strings.Contains(got, "5") {
			t.Errorf("should not contain line 5: %q", got)
		}
		if !strings.HasSuffix(got, "...") {
			t.Errorf("missing suffix: %q", got)
		}
	})
}

func TestTruncateHexValue(t *testing.T) {
	long := "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969"
	if got := truncateHexValue(long); got != "185f8db3" {
		t.Errorf("truncateHexValue(%q) = %q, want %q", long, got, "185f8db3")
	}
	short := "text/plain"
	if got := truncateHexValue(short); got != short {
		t.Errorf("truncateHexValue(%q) = %q, want unchanged", short, got)
	}
}
