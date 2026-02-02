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

func TestFederateCmd_Success(t *testing.T) {
	var gotPeer string
	var gotLabels map[string]string

	mc := &mockClient{
		federateFn: func(_ context.Context, peer string, labels map[string]string) (*client.FederateResult, error) {
			gotPeer = peer
			gotLabels = labels
			return &client.FederateResult{Message: "federation started"}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newFederateCmd(n)
	cmd.SetArgs([]string{"peer.example.com:50051", "-l", "app=journal"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPeer != "peer.example.com:50051" {
		t.Errorf("peer = %q, want %q", gotPeer, "peer.example.com:50051")
	}
	if gotLabels["app"] != "journal" {
		t.Errorf("labels = %v, want app=journal", gotLabels)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "federation started") {
		t.Fatalf("expected message in output, got %q", buf.String())
	}
}

func TestFederateCmd_NormalizesURL(t *testing.T) {
	var gotPeer string

	mc := &mockClient{
		federateFn: func(_ context.Context, peer string, _ map[string]string) (*client.FederateResult, error) {
			gotPeer = peer
			return nil, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newFederateCmd(n)
	cmd.SetArgs([]string{"grpc://peer.example.com:50051"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPeer != "peer.example.com:50051" {
		t.Errorf("peer = %q, want %q", gotPeer, "peer.example.com:50051")
	}
}

func TestFederateCmd_JSONOutput(t *testing.T) {
	mc := &mockClient{
		federateFn: func(_ context.Context, _ string, _ map[string]string) (*client.FederateResult, error) {
			return &client.FederateResult{Status: "active", Message: "connected"}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newFederateCmd(n)
	cmd.SetArgs([]string{"peer:50051", "-o", "json"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "active") {
		t.Fatalf("expected JSON output with status, got %q", buf.String())
	}
}

func TestFederateCmd_NilResult(t *testing.T) {
	mc := &mockClient{
		federateFn: func(_ context.Context, _ string, _ map[string]string) (*client.FederateResult, error) {
			return nil, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newFederateCmd(n)
	cmd.SetArgs([]string{"peer:50051"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "ok") {
		t.Fatalf("expected 'ok' output, got %q", buf.String())
	}
}

func TestFederateCmd_StatusOnly(t *testing.T) {
	mc := &mockClient{
		federateFn: func(_ context.Context, _ string, _ map[string]string) (*client.FederateResult, error) {
			return &client.FederateResult{Status: "already_active"}, nil
		},
	}

	n := &nodeCmd{client: mc}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd := newFederateCmd(n)
	cmd.SetArgs([]string{"peer:50051"})
	err := cmd.Execute()

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	if !strings.Contains(buf.String(), "already_active") {
		t.Fatalf("expected status output, got %q", buf.String())
	}
}

func TestFederateCmd_Error(t *testing.T) {
	mc := &mockClient{
		federateFn: func(_ context.Context, _ string, _ map[string]string) (*client.FederateResult, error) {
			return nil, fmt.Errorf("already federated")
		},
	}

	n := &nodeCmd{client: mc}

	cmd := newFederateCmd(n)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"peer:50051"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
}
