package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gezibash/arc-node/pkg/client"
)

func TestStatusCmd_Online(t *testing.T) {
	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, opts *client.QueryOptions) (*client.QueryResult, error) {
			if opts.Limit != 1 {
				t.Errorf("limit = %d, want 1", opts.Limit)
			}
			return &client.QueryResult{}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newStatusCmd(n)
	cmd.SetArgs([]string{})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "online") {
		t.Fatalf("expected 'online' in output, got %q", buf.String())
	}
}

func TestStatusCmd_Unreachable(t *testing.T) {
	mc := &mockClient{
		queryMessagesFn: func(_ context.Context, _ *client.QueryOptions) (*client.QueryResult, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newStatusCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err == nil {
		t.Fatal("expected error")
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "unreachable") {
		t.Fatalf("expected 'unreachable' in output, got %q", buf.String())
	}
}
