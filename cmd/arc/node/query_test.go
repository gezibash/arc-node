package node

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestQueryCmd_Success(t *testing.T) {
	ref := reference.Compute([]byte("entry"))

	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, opts *client.QueryOptions) (*client.QueryResult, error) {
			if opts.Expression != "true" {
				t.Errorf("expression = %q, want %q", opts.Expression, "true")
			}
			return &client.QueryResult{
				Entries: []*client.Entry{
					{Ref: ref, Labels: map[string]string{"app": "test"}, Timestamp: 1700000000000},
				},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newQueryCmd(n)
	cmd.SetArgs([]string{})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if !strings.Contains(out, reference.Hex(ref)[:8]) {
		t.Fatalf("expected short ref in output, got %q", out)
	}
}

func TestQueryCmd_WithLabels(t *testing.T) {
	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, opts *client.QueryOptions) (*client.QueryResult, error) {
			if opts.Labels["app"] != "journal" {
				t.Errorf("labels = %v, want app=journal", opts.Labels)
			}
			if opts.Expression != `labels["app"] == "journal"` {
				t.Errorf("expression = %q", opts.Expression)
			}
			return &client.QueryResult{}, nil
		},
	}

	n := &nodeCmd{client: mc}

	cmd := newQueryCmd(n)
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{`labels["app"] == "journal"`, "-l", "app=journal"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryCmd_EmptyResults(t *testing.T) {
	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, _ *client.QueryOptions) (*client.QueryResult, error) {
			return &client.QueryResult{}, nil
		},
	}

	n := &nodeCmd{client: mc}

	cmd := newQueryCmd(n)
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryCmd_JSONOutput(t *testing.T) {
	ref := reference.Compute([]byte("json-entry"))

	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, _ *client.QueryOptions) (*client.QueryResult, error) {
			return &client.QueryResult{
				Entries: []*client.Entry{
					{Ref: ref, Labels: map[string]string{"app": "test"}, Timestamp: 1700000000000},
				},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newQueryCmd(n)
	cmd.SetArgs([]string{"-o", "json"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if !strings.Contains(out, `"reference"`) {
		t.Fatalf("expected JSON output, got %q", out)
	}
}

func TestQueryCmd_HasMore(t *testing.T) {
	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, _ *client.QueryOptions) (*client.QueryResult, error) {
			return &client.QueryResult{
				HasMore:    true,
				NextCursor: "abc123",
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	cmd := newQueryCmd(n)
	cmd.SetArgs([]string{})
	err := cmd.Execute()

	w.Close()
	os.Stderr = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "abc123") {
		t.Fatalf("expected cursor in stderr, got %q", buf.String())
	}
}
