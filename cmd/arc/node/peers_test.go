package node

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
)

func TestPeersCmd_ListPeers(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return []client.PeerInfo{
				{
					Address:           "peer1:50051",
					Labels:            map[string]string{"app": "journal"},
					BytesReceived:     1024,
					EntriesSent:       10,
					EntriesReplicated: 5,
					StartedAt:         time.Now().Add(-time.Hour).UnixMilli(),
					Direction:         client.PeerDirectionOutbound,
				},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
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
	if !strings.Contains(out, "peer1:50051") {
		t.Fatalf("expected peer address in output, got %q", out)
	}
}

func TestPeersCmd_EmptyPeers(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return nil, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
	cmd.SetArgs([]string{})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "no active peers") {
		t.Fatalf("expected 'no active peers', got %q", buf.String())
	}
}

func TestPeersCmd_FilterInbound(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return []client.PeerInfo{
				{Address: "out:50051", Direction: client.PeerDirectionOutbound, StartedAt: time.Now().UnixMilli()},
				{PublicKey: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Direction: client.PeerDirectionInbound, StartedAt: time.Now().UnixMilli()},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
	cmd.SetArgs([]string{"inbound"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if strings.Contains(out, "out:50051") {
		t.Fatalf("should not contain outbound peer, got %q", out)
	}
}

func TestPeersCmd_InvalidDirection(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return nil, nil
		},
	}

	n := &nodeCmd{client: mc}

	cmd := newPeersCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"sideways"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid direction")
	}
}

func TestPeersCmd_JSONOutput(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return []client.PeerInfo{
				{Address: "peer1:50051", Direction: client.PeerDirectionOutbound, StartedAt: time.Now().UnixMilli()},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
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
	if !strings.Contains(out, "peer1:50051") {
		t.Fatalf("expected peer in JSON output, got %q", out)
	}
}

func TestPeersCmd_JSONFilteredInbound(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return []client.PeerInfo{
				{Address: "out:50051", Direction: client.PeerDirectionOutbound, StartedAt: time.Now().UnixMilli()},
				{PublicKey: []byte{1}, Direction: client.PeerDirectionInbound, StartedAt: time.Now().UnixMilli()},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
	cmd.SetArgs([]string{"inbound", "-o", "json"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if strings.Contains(out, "out:50051") {
		t.Fatalf("JSON inbound filter should exclude outbound, got %q", out)
	}
}

func TestPeersCmd_JSONFilteredOutbound(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return []client.PeerInfo{
				{Address: "out:50051", Direction: client.PeerDirectionOutbound, StartedAt: time.Now().UnixMilli()},
				{PublicKey: []byte{1}, Direction: client.PeerDirectionInbound, StartedAt: time.Now().UnixMilli()},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
	cmd.SetArgs([]string{"outbound", "-o", "json"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if !strings.Contains(out, "out:50051") {
		t.Fatalf("expected outbound peer in JSON, got %q", out)
	}
}

func TestPeersCmd_FilterOutbound(t *testing.T) {
	mc := &mockClient{
		listPeersFn: func(_ context.Context) ([]client.PeerInfo, error) {
			return []client.PeerInfo{
				{Address: "out:50051", Direction: client.PeerDirectionOutbound, StartedAt: time.Now().UnixMilli()},
				{PublicKey: []byte{1, 2, 3}, Direction: client.PeerDirectionInbound, StartedAt: time.Now().UnixMilli()},
			}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newPeersCmd(n)
	cmd.SetArgs([]string{"outbound"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	out := buf.String()
	if !strings.Contains(out, "out:50051") {
		t.Fatalf("expected outbound peer, got %q", out)
	}
}

func TestNormalizePeerAddr(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"peer:50051", "peer:50051"},
		{"grpc://peer:50051", "peer:50051"},
		{"http://peer:50051", "peer:50051"},
	}
	for _, tt := range tests {
		got := normalizePeerAddr(tt.input)
		if got != tt.want {
			t.Errorf("normalizePeerAddr(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatLabels(t *testing.T) {
	got := formatLabels(map[string]string{"b": "2", "a": "1"})
	if got != "a=1 b=2" {
		t.Errorf("formatLabels = %q, want %q", got, "a=1 b=2")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500 B"},
		{1500, "1.5 kB"},
		{1500000, "1.5 MB"},
		{1500000000, "1.5 GB"},
	}
	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
