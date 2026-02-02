package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/reference"
)

func TestWatchCmd_ReceivesMessages(t *testing.T) {
	ref := reference.Compute([]byte("watch-entry"))

	entries := make(chan *client.Entry, 1)
	errs := make(chan error, 1)

	entries <- &client.Entry{
		Ref:       ref,
		Labels:    map[string]string{"app": "live"},
		Timestamp: 1700000000000,
	}
	close(entries)

	mc := &mockClient{
		subscribeMessagesFn: func(_ context.Context, expr string, labels map[string]string) (<-chan *client.Entry, <-chan error, error) {
			if expr != "true" {
				t.Errorf("expression = %q, want %q", expr, "true")
			}
			return entries, errs, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newWatchCmd(n)
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

func TestWatchCmd_SubscribeError(t *testing.T) {
	mc := &mockClient{
		subscribeMessagesFn: func(_ context.Context, _ string, _ map[string]string) (<-chan *client.Entry, <-chan error, error) {
			return nil, nil, fmt.Errorf("connection lost")
		},
	}

	n := &nodeCmd{client: mc}

	cmd := newWatchCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWatchCmd_StreamError(t *testing.T) {
	entries := make(chan *client.Entry)
	errs := make(chan error, 1)
	errs <- fmt.Errorf("stream broken")

	mc := &mockClient{
		subscribeMessagesFn: func(_ context.Context, _ string, _ map[string]string) (<-chan *client.Entry, <-chan error, error) {
			return entries, errs, nil
		},
	}

	n := &nodeCmd{client: mc}

	cmd := newWatchCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}
